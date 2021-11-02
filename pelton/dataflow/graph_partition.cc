#include "pelton/dataflow/graph_partition.h"

#include <memory>
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
  // Clone each node.
  for (size_t i = 0; i < this->nodes_.size(); i++) {
    const std::unique_ptr<Operator> &node = this->nodes_.at(i);
    // Find the cloned parents.
    std::vector<Operator *> parents;
    parents.reserve(node->parents().size());
    for (Operator *parent : node->parents()) {
      parents.push_back(partition->GetNode(parent->index()));
    }
    // Clone the operator.
    std::unique_ptr<Operator> cloned = node->Clone();
    // Add cloned operator to this partition.
    switch (this->nodes_.at(i)->type()) {
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
