#include "pelton/dataflow/graph.h"

#include <memory>

#include "glog/logging.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"

namespace pelton {
namespace dataflow {

// DataFlowGraphPartition.
bool DataFlowGraphPartition::AddInputNode(std::shared_ptr<InputOperator> op) {
  CHECK(this->inputs_.count(op->input_name()) == 0)
      << "An operator for this input already exists";
  this->inputs_.emplace(op->input_name(), op);
  return this->AddNode(op, std::vector<std::shared_ptr<Operator>>{});
}
bool DataFlowGraphPartition::AddOutputOperator(
    std::shared_ptr<MatViewOperator> op, std::shared_ptr<Operator> parent) {
  this->outputs_.emplace_back(op);
  return this->AddNode(op, parent);
}
bool DataFlowGraphPartition::AddNode(
    std::shared_ptr<Operator> op,
    std::vector<std::shared_ptr<Operator>> parents) {
  NodeIndex idx = this->nodes_.size();
  op->AddToPartition(this, idx);

  const auto &[_, inserted] = this->nodes_.emplace(idx, op);
  CHECK(inserted);

  for (const auto &parent : parents) {
    op->AddParent(parent);
  }

  return inserted;
}

void DataFlowGraphPartition::Process(const std::string &input_name,
                                     std::vector<Record> &&records) {
  std::shared_ptr<InputOperator> node = this->inputs_.at(input_name);
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

std::unique_ptr<DataFlowGraphPartition> DataFlowGraphPartition::Clone() const {
  auto partition = std::make_unique<DataFlowGraphPartition>();
  // Clone each node.
  for (size_t i = 0; i < this->nodes_.size(); i++) {
    std::shared_ptr<Operator> node = this->nodes_.at(i)->Clone();
    node->AddToPartition(partition.get(), i);
    partition->nodes_.emplace(i, node);

    // Must also store input and matview operators in special maps.
    switch (this->nodes_.at(i)->type()) {
      case Operator::Type::INPUT: {
        auto input = std::static_pointer_cast<InputOperator>(node);
        partition->inputs_.emplace(input->input_name(), input);
        break;
      }
      case Operator::Type::MAT_VIEW: {
        auto matview = std::static_pointer_cast<MatViewOperator>(node);
        partition->outputs_.push_back(matview);
        break;
      }
      default: {
      }
    }
  }
  return partition;
}

// DataFlowGraph.
void DataFlowGraph::Initialize(
    std::unique_ptr<DataFlowGraphPartition> &&partition,
    PartitionIndex partitions) {
  // Store the input names for this graph (i.e. for any partition).
  for (const auto &[input_name, _] : partition->inputs()) {
    this->inputs_.push_back(input_name);
  }

  // Make the specified number of partitions.
  for (PartitionIndex i = 0; i < partitions - 1; i++) {
    this->partitions_.push_back(partition->Clone());
  }
  this->partitions_.push_back(std::move(partition));
}

void DataFlowGraph::Process(const std::string &input_name,
                            std::vector<Record> &&records) {
  this->partitions_.front()->Process(input_name, std::move(records));
}

}  // namespace dataflow
}  // namespace pelton
