#include "pelton/dataflow/ops/exchange.h"

#include <memory>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/partition.h"
#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

void ExchangeOperator::InitChanIndices() {
  // Set the source as the current exchange op's index
  NodeIndex source_index = this->index();
  // Set the "destination" node for the message as the child of this
  // exchange operator.
  CHECK_EQ(this->children_.size(), static_cast<size_t>(1));
  NodeIndex destination_index = this->children_.at(0);
  for (const auto &[_, chan] : this->partition_chans_) {
    chan->SetSourceIndex(source_index);
    chan->SetDestinationIndex(destination_index);
  }
}

void ExchangeOperator::ComputeOutputSchema() {
  this->output_schema_ = this->input_schemas_.at(0);
}

std::optional<std::vector<Record>> ExchangeOperator::Process(
    NodeIndex, const std::vector<Record> &records) {
  std::vector<Record> output;
  // Return if no records to process
  if (records.size() == 0) {
    return std::move(output);
  }

  absl::flat_hash_map<PartitionID, std::vector<Record>> partitioned_records =
      partition::HashPartition(records, this->partition_key_,
                               this->total_partitions_);

  // Forward records that are meant to be in the current partition
  if (partitioned_records.contains(this->current_partition_)) {
    for (auto &record : partitioned_records.at(this->current_partition_)) {
      output.push_back(std::move(record));
    }
  }

  // Send batches to appropriate peers
  for (auto &item : partitioned_records) {
    if (item.first == this->current_partition_) {
      continue;
    }
    auto msg = std::make_shared<BatchMessage>(std::move(item.second));
    this->partition_chans_.at(item.first)->Send(msg);
  }
  return std::move(output);
}

}  // namespace dataflow
}  // namespace pelton
