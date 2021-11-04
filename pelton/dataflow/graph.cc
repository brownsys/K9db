#include "pelton/dataflow/graph.h"

#include <algorithm>
#include <tuple>
#include <utility>
// NOLINTNEXTLINE
#include <variant>

#include "glog/logging.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/aggregate.h"
#include "pelton/dataflow/ops/equijoin.h"
#include "pelton/dataflow/ops/exchange.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/ops/project.h"
#include "pelton/dataflow/schema.h"

namespace pelton {
namespace dataflow {

// To avoid clutter, everything related to identifying what operator is
// partitioned by what key is in this file.
#include "pelton/dataflow/graph_traversal.inc"

// DataFlowGraph.
void DataFlowGraph::Initialize(
    std::unique_ptr<DataFlowGraphPartition> &&partition,
    PartitionIndex partitions) {
  // Store the input names for this graph (i.e. for any partition).
  for (const auto &[input_name, _] : partition->inputs()) {
    this->inputs_.push_back(input_name);
  }

  // Traverse the graph, adding the needed exchange operators, and deducing
  // the partitioning keys for the inputs.
  ProducerMap p;
  RequiredMap r;
  ExternalMap e;
  for (const auto &[_, input_operator] : partition->inputs()) {
    DFS(*input_operator, UNDEFINED_NODE_INDEX, Any::UNIT, &p, &r, e);
  }

  // Check to see that every MatView was assigned a partition key.
  // If some were not, assign them one externally.
  if (ExternallyAssignKey(partition->outputs(), p, &e)) {
    // Run again with the new external constraints
    p.clear();
    r.clear();
    for (const auto &[_, input_operator] : partition->inputs()) {
      DFS(*input_operator, UNDEFINED_NODE_INDEX, Any::UNIT, &p, &r, e);
    }
  }

  // Check to see that every input operator was assigned a partition key.
  // If some were not, assign them one externally.
  std::vector<InputOperator *> inputs(partition->inputs().size());
  std::transform(partition->inputs().begin(), partition->inputs().end(),
                 inputs.begin(), [](auto pair) { return pair.second; });
  if (ExternallyAssignKey(inputs, p, &e)) {
    p.clear();
    r.clear();
    for (const auto &[_, input_operator] : partition->inputs()) {
      DFS(*input_operator, UNDEFINED_NODE_INDEX, Any::UNIT, &p, &r, e);
    }
  }

  // Printing.
  DebugPrint(partition.get(), p, r);

  // Fill in partition key maps.
  for (const auto &[name, op] : partition->inputs()) {
    this->inkeys_.emplace(name, std::get<PartitionKey>(p.at(op->index())));
  }
  for (auto op : partition->outputs()) {
    this->outkeys_.push_back(std::get<PartitionKey>(p.at(op->index())));
  }

  // Add in any needed exchanges.
  std::vector<std::tuple<PartitionKey, Operator *, Operator *>> exchanges;
  for (NodeIndex i = 0; i < partition->Size(); i++) {
    Operator *op = partition->GetNode(i);
    for (Operator *child : op->children()) {
      auto result = ExchangeKey(op, child, p, r);
      if (result) {
        exchanges.emplace_back(std::move(result.value()), op, child);
      }
    }
  }
  for (const auto &[key, parent, child] : exchanges) {
    partition->InsertNode(
        std::make_unique<ExchangeOperator>(&this->partitions_, key), parent,
        child);
  }

  std::cout << partition->DebugString() << std::endl;

  // Make the specified number of partitions.
  for (PartitionIndex i = 0; i < partitions; i++) {
    this->partitions_.push_back(partition->Clone(i));
  }
}

void DataFlowGraph::Process(const std::string &input_name,
                            std::vector<Record> &&records) {
  this->partitions_.front()->Process(input_name, std::move(records));
}

PartitionIndex DataFlowGraph::Partition(const std::vector<ColumnID> &key,
                                        const Record &record) const {
  return record.Hash(key) % this->partitions_.size();
}

void DataFlowGraph::SendToPartition(PartitionIndex partition, NodeIndex node,
                                    std::vector<Record> &&records) const {
  this->partitions_.at(partition)->GetNode(node)->ProcessAndForward(
      node, std::move(records));
}

}  // namespace dataflow
}  // namespace pelton
