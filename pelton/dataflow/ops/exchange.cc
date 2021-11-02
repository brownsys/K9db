#include "pelton/dataflow/ops/exchange.h"

#include <unordered_map>
#include <utility>

namespace pelton {
namespace dataflow {

std::vector<Record> ExchangeOperator::Process(NodeIndex source,
                                              std::vector<Record> &&records) {
  // Records are sent from parallel exchange operators in other partitions,
  // we do not need to try to partition them again. We know they belong
  // to our partition!
  if (source == this->index()) {
    return std::move(records);
  }

  // Map each input record to its output partition.
  std::unordered_map<PartitionIndex, std::vector<Record>> partitioned;
  for (Record &record : records) {
    PartitionIndex partition = this->graph_->Partition(this->key_, record);
    partitioned[partition].push_back(std::move(record));
  }

  // Send records to each partition.
  for (auto &[partition, records] : partitioned) {
    if (partition != this->partition()) {
      this->graph_->SendToPartition(partition, source, std::move(records));
    }
  }

  return std::move(partitioned.at(this->partition()));
}

void ExchangeOperator::ComputeOutputSchema() {
  this->output_schema_ = this->input_schemas_.at(0);
}

std::unique_ptr<Operator> ExchangeOperator::Clone() const {
  return std::make_unique<ExchangeOperator>(this->graph_, this->key_);
}

}  // namespace dataflow
}  // namespace pelton
