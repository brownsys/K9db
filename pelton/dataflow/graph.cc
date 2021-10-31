#include "pelton/dataflow/graph.h"

#include <utility>

#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"

namespace pelton {
namespace dataflow {

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
