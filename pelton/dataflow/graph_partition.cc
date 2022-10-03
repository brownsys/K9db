#include "pelton/dataflow/graph_partition.h"

#include <memory>
#include <queue>
#include <set>
#include <unordered_set>
#include <utility>

#include "glog/logging.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/forward_view.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"

namespace pelton {
namespace dataflow {

namespace {

template <typename to, typename from>
inline std::unique_ptr<to> UniqueCast(std::unique_ptr<from> &&ptr) {
  return std::unique_ptr<to>(static_cast<to *>(ptr.release()));
}

}  // namespace

// DataFlowGraphPartition.
bool DataFlowGraphPartition::AddInputNode(std::unique_ptr<InputOperator> &&op,
                                          NodeIndex idx) {
  CHECK(this->inputs_.count(op->input_name()) == 0)
      << "An operator for this input already exists";
  this->inputs_.emplace(op->input_name(), op.get());
  return this->AddNode(std::move(op), idx, std::vector<Operator *>{});
}
bool DataFlowGraphPartition::AddForwardOperator(
    std::unique_ptr<ForwardViewOperator> &&op, NodeIndex idx,
    const std::vector<Operator *> &parents) {
  this->forwards_.emplace_back(op.get());
  return this->AddNode(std::move(op), idx, parents);
}
bool DataFlowGraphPartition::AddOutputOperator(
    std::unique_ptr<MatViewOperator> &&op, NodeIndex idx, Operator *parent) {
  this->outputs_.emplace_back(op.get());
  return this->AddNode(std::move(op), idx, std::vector<Operator *>{parent});
}
bool DataFlowGraphPartition::AddNode(std::unique_ptr<Operator> &&op,
                                     NodeIndex idx,
                                     const std::vector<Operator *> &parents) {
  op->SetIndex(idx);
  op->SetPartition(this->id_);
  for (Operator *parent : parents) {
    op->AddParent(parent);
  }

  const auto &[_, inserted] = this->nodes_.emplace(idx, std::move(op));
  CHECK(inserted);
  return inserted;
}

bool DataFlowGraphPartition::InsertNode(std::unique_ptr<Operator> &&op,
                                        Operator *parent, Operator *child) {
  // Remove the edge between parent and child.
  size_t parent_index = child->RemoveParent(parent);
  // Initialize op.
  NodeIndex idx = this->nodes_.size();
  op->SetIndex(idx);
  op->SetPartition(this->id_);
  // Put op between parent and child.
  op->AddParent(parent);
  child->AddParentAt(op.get(), parent_index);

  const auto &[_, inserted] = this->nodes_.emplace(idx, std::move(op));
  CHECK(inserted);
  return inserted;
  return true;
}

void DataFlowGraphPartition::_Process(const std::string &input_name,
                                      std::vector<Record> &&records) {
  InputOperator *node = this->inputs_.at(input_name);
  node->ProcessAndForward(UNDEFINED_NODE_INDEX, std::move(records),
                          Promise::None.Derive());
}

std::string DataFlowGraphPartition::DebugString() const {
  // Go over nodes in order (of index).
  std::set<NodeIndex> ordered_nodes;
  for (const auto &[idx, _] : this->nodes_) {
    ordered_nodes.insert(idx);
  }

  std::string str = "[\n";
  for (auto idx : ordered_nodes) {
    const auto &node = this->nodes_.at(idx);
    str += " {\n";
    str += node->DebugString();
    str += " },\n";
  }
  str += "]";
  return str;
}
std::vector<Record> DataFlowGraphPartition::DebugRecords() const {
  std::vector<Record> records;
  // Go over nodes in order (of index).
  std::set<NodeIndex> ordered_nodes;
  for (const auto &[idx, _] : this->nodes_) {
    ordered_nodes.insert(idx);
  }
  for (auto idx : ordered_nodes) {
    records.push_back(this->nodes_.at(idx)->DebugRecord());
  }
  return records;
}

uint64_t DataFlowGraphPartition::SizeInMemory(
    const std::string &flow_name, std::vector<Record> *output) const {
  uint64_t size = 0;
  for (const auto &[_, node] : this->nodes_) {
    size += node->SizeInMemory(flow_name, output);
  }
  return size;
}

std::unique_ptr<DataFlowGraphPartition> DataFlowGraphPartition::Clone(
    PartitionIndex partition_index,
    const std::unordered_map<std::string, std::vector<Operator *>> &ps) const {
  auto partition = std::make_unique<DataFlowGraphPartition>(partition_index);

  // Cloning is a BFS starting from inputs and follows NodeIndex.
  // 1. Every parent must be cloned before any child.
  // 2. Children of a node must be visited from the smallest to largest index.
  std::queue<Operator *> BFS;
  for (const auto &[_, input] : this->inputs_) {
    BFS.push(input);
  }
  for (const auto &forward : this->forwards_) {
    BFS.push(forward);
  }

  // Clone each node.
  std::unordered_set<NodeIndex> visited;
  while (!BFS.empty()) {
    Operator *node = BFS.front();
    BFS.pop();

    // Skip already cloned nodes.
    NodeIndex idx = node->index();
    if (visited.count(idx) == 1) {
      continue;
    }

    // Find the cloned parents.
    std::vector<Operator *> parents;
    parents.reserve(node->parents().size());
    for (Operator *parent : node->parents()) {
      if (visited.count(parent->index())) {
        parents.push_back(partition->GetNode(parent->index()));
      } else {
        break;
      }
    }
    // If some of the parents were not cloned, skip this node.
    // We will get back to it when its last parent is cloned.
    if (parents.size() != node->parents().size()) {
      continue;
    }
    // Add Children to BFS.
    visited.insert(idx);
    for (Operator *child : node->children()) {
      BFS.push(child);
    }
    // Clone the operator.
    std::unique_ptr<Operator> cloned = node->Clone();
    // Add cloned operator to this partition.
    switch (cloned->type()) {
      case Operator::Type::INPUT: {
        auto input = UniqueCast<InputOperator>(std::move(cloned));
        partition->AddInputNode(std::move(input), idx);
        break;
      }
      case Operator::Type::FORWARD_VIEW: {
        auto forward = UniqueCast<ForwardViewOperator>(std::move(cloned));
        const auto &parent_views = ps.at(forward->parent_flow());
        Operator *parent = parent_views.at(partition_index);
        partition->AddForwardOperator(std::move(forward), idx, {parent});
        break;
      }
      case Operator::Type::MAT_VIEW: {
        auto output = UniqueCast<MatViewOperator>(std::move(cloned));
        partition->AddOutputOperator(std::move(output), idx, parents.at(0));
        break;
      }
      default: {
        partition->AddNode(std::move(cloned), idx, parents);
        break;
      }
    }
  }
  return partition;
}

}  // namespace dataflow
}  // namespace pelton
