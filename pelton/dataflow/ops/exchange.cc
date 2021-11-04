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
    PartitionIndex partition =
        record.Hash(this->outkey_) % this->partitions_->size();
    partitioned[partition].push_back(std::move(record));
  }

  // Send records to each partition.
  for (auto &[partition, records] : partitioned) {
    if (partition != this->partition()) {
      Operator *exchange = partitions_->at(partition)->GetNode(this->index());
      exchange->ProcessAndForward(this->index(), std::move(records));
    }
  }

  return std::move(partitioned.at(this->partition()));
}

void ExchangeOperator::ComputeOutputSchema() {
  this->output_schema_ = this->input_schemas_.at(0);
}

std::unique_ptr<Operator> ExchangeOperator::Clone() const {
  return std::make_unique<ExchangeOperator>(this->partitions_, this->outkey_);
}

}  // namespace dataflow
}  // namespace pelton
