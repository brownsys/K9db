#include "pelton/dataflow/graph.h"

#include <algorithm>
#include <utility>
// NOLINTNEXTLINE
#include <variant>

#include "glog/logging.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/aggregate.h"
#include "pelton/dataflow/ops/equijoin.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/ops/project.h"

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
