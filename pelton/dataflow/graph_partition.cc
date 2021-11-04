#include "pelton/dataflow/graph_partition.h"

#include <functional>
#include <memory>
#include <queue>
#include <unordered_set>
#include <utility>

#include "glog/logging.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"

namespace pelton {
namespace dataflow {

namespace {

template <typename to, typename from>
inline std::unique_ptr<to> unique_cast(std::unique_ptr<from> &&ptr) {
  return std::unique_ptr<to>(static_cast<to *>(ptr.release()));
}

using MinQueue = std::priority_queue<NodeIndex, std::vector<NodeIndex>,
                                     std::greater<NodeIndex>>;

}  // namespace

// DataFlowGraphPartition.
bool DataFlowGraphPartition::AddInputNode(std::unique_ptr<InputOperator> &&op) {
  CHECK(this->inputs_.count(op->input_name()) == 0)
      << "An operator for this input already exists";
  this->inputs_.emplace(op->input_name(), op.get());
  return this->AddNode(std::move(op), std::vector<Operator *>{});
}
bool DataFlowGraphPartition::AddOutputOperator(
    std::unique_ptr<MatViewOperator> &&op, Operator *parent) {
  this->outputs_.emplace_back(op.get());
  return this->AddNode(std::move(op), parent);
}
bool DataFlowGraphPartition::AddNode(std::unique_ptr<Operator> &&op,
                                     const std::vector<Operator *> &parents) {
  NodeIndex idx = this->nodes_.size();
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
  child->RemoveParent(parent);
  // Initialize op.
  NodeIndex idx = this->nodes_.size();
  op->SetIndex(idx);
  op->SetPartition(this->id_);
  // Put op between parent and child.
  op->AddParent(parent);
  child->AddParent(op.get());

  const auto &[_, inserted] = this->nodes_.emplace(idx, std::move(op));
  CHECK(inserted);
  return inserted;
  return true;
}

void DataFlowGraphPartition::Process(const std::string &input_name,
                                     std::vector<Record> &&records) {
  InputOperator *node = this->inputs_.at(input_name);
  node->ProcessAndForward(UNDEFINED_NODE_INDEX, std::move(records));
}

std::string DataFlowGraphPartition::DebugString() const {
  std::string str = "[\n";
  for (const auto &[_, node] : this->nodes_) {
    str += " {\n";
    str += node->DebugString();
    str += " },\n";
  }
  str += "]";
  return str;
}
uint64_t DataFlowGraphPartition::SizeInMemory() const {
  uint64_t size = 0;
  for (const auto &[_, node] : this->nodes_) {
    size += node->SizeInMemory();
  }
  return size;
}

std::unique_ptr<DataFlowGraphPartition> DataFlowGraphPartition::Clone(
    PartitionIndex partition_index) const {
  auto partition = std::make_unique<DataFlowGraphPartition>(partition_index);

  // Cloning is a BFS starting from inputs and follows NodeIndex.
  // 1. Every parent must be cloned before any child.
  // 2. Children of a node must be visited from the smallest to largest index.
  MinQueue BFS;
  for (const auto &[_, input] : this->inputs_) {
    BFS.push(input->index());
  }

  // Clone each node.
  std::unordered_set<NodeIndex> visited;
  while (!BFS.empty()) {
    Operator *node = this->GetNode(BFS.top());
    BFS.pop();
    // Skip already cloned nodes.
    if (visited.count(node->index()) == 1) {
      continue;
    }
    // Find the cloned parents.
    std::vector<Operator *> parents;
    parents.reserve(node->parents().size());
    for (Operator *parent : node->parents()) {
      if (partition->nodes_.count(parent->index()) == 1) {
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
    for (Operator *child : node->children()) {
      BFS.push(child->index());
    }
    // Clone the operator.
    std::unique_ptr<Operator> cloned = node->Clone();
    visited.insert(node->index());
    // Add cloned operator to this partition.
    switch (cloned->type()) {
      case Operator::Type::INPUT: {
        auto input = unique_cast<InputOperator>(std::move(cloned));
        partition->AddInputNode(std::move(input));
        break;
      }
      case Operator::Type::MAT_VIEW: {
        auto output = unique_cast<MatViewOperator>(std::move(cloned));
        partition->AddOutputOperator(std::move(output), parents.at(0));
        break;
      }
      default: {
        partition->AddNode(std::move(cloned), parents);
        break;
      }
    }
  }
  return partition;
}

}  // namespace dataflow
}  // namespace pelton
