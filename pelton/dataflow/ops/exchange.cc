#include "pelton/dataflow/ops/exchange.h"

#include <unordered_map>
#include <utility>

namespace pelton {
namespace dataflow {

std::vector<Record> ExchangeOperator::Process(
    NodeIndex source, std::vector<Record> &&records,
    std::optional<Promise> &&promise) {
  // Records are sent from parallel exchange operators in other partitions,
  // we do not need to try to partition them again. We know they belong
  // to our partition!
  if (source == this->index()) {
    // Set this promise, a parent promise still exists in the parent
    // ProcessAndForward API.
    if (promise) {
      promise.value().Set();
    }
    return std::move(records);
  }

  // Map each input record to its output partition.
  std::unordered_map<PartitionIndex, std::vector<Record>> partitioned;
  for (Record &record : records) {
    PartitionIndex partition = record.Hash(this->outkey_) % this->partitions_;
    partitioned[partition].push_back(std::move(record));
  }

  // Send records to each partition.
  for (auto &[partition, records] : partitioned) {
    if (partition != this->partition()) {
      if (promise) {
        this->channels_.at(partition)->Write(
            this->partition(),
            {this->flow_name_, std::move(records), this->index(), this->index(),
             promise.value().Derive()});
      } else {
        this->channels_.at(partition)->Write(
            this->partition(), {this->flow_name_, std::move(records),
                                this->index(), this->index(), std::nullopt});
      }
    }
  }
  // Set parent promise.
  if (promise) {
    promise.value().Set();
  }

  return std::move(partitioned[this->partition()]);
}

void ExchangeOperator::ComputeOutputSchema() {
  this->output_schema_ = this->input_schemas_.at(0);
}

std::unique_ptr<Operator> ExchangeOperator::Clone() const {
  return std::make_unique<ExchangeOperator>(this->flow_name_, this->partitions_,
                                            this->channels_, this->outkey_);
}

}  // namespace dataflow
}  // namespace pelton
