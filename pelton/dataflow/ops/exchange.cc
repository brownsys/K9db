#include "pelton/dataflow/ops/exchange.h"

#include <memory>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

void ExchangeOperator::ComputeOutputSchema() {
  this->output_schema_ = this->input_schemas_.at(0);
}

bool ExchangeOperator::Process(NodeIndex source,
                               const std::vector<Record> &records,
                               std::vector<Record> *output) {
  // Return if no records to process
  if (records.size() == 0) {
    return true;
  }
  // Input records will have to be copied since it is a const reference.
  // TODO(Ishan): Benchmark this later with the slice representation
  std::vector<Record> to_partition;
  for (auto const &record : records) {
    to_partition.push_back(record.Copy());
  }

  absl::flat_hash_map<uint16_t, std::vector<Record>> partitioned_records =
      PartitionTrivial(std::move(to_partition), this->partition_key_,
                       this->total_partitions_);

  // Forward records that are meant to be in the current partition
  if (partitioned_records.contains(this->current_partition_)) {
    for (auto &record : partitioned_records.at(this->current_partition_)) {
      output->push_back(std::move(record));
    }
  }

  // Send batches to appropriate peers
  for (auto &item : partitioned_records) {
    if (item.first == this->current_partition_) {
      continue;
    }
    // Set the "entry" node for the message as the child of this
    // exchange operator.
    NodeIndex entry_index =
        this->graph()->GetNode(this->children_.at(0))->index();
    // Set the source as the current node's index.
    NodeIndex source_index = this->index();
    auto msg = std::make_shared<BatchMessage>(entry_index, source_index,
                                              std::move(item.second));
    this->partition_chans_.at(item.first)->Send(msg);
  }
  return true;
}

}  // namespace dataflow
}  // namespace pelton
