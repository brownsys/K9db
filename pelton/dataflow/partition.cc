#include "pelton/dataflow/partition.h"

#include <stdlib.h>

#include <utility>

namespace pelton {
namespace dataflow {

absl::flat_hash_map<uint16_t, std::vector<Record>> PartitionTrivial(
    std::vector<Record> records, std::vector<ColumnID> cols,
    uint16_t nthreads) {
  absl::flat_hash_map<uint16_t, std::vector<Record>> partitions;
  for (auto &record : records) {
    size_t hash = record.Hash(cols);
    uint16_t partition = (uint16_t)(hash % (size_t)nthreads);
    if (partitions.contains(partition)) {
      partitions.at(partition).push_back(std::move(record));
    } else {
      std::vector<Record> records;
      records.push_back(std::move(record));
      partitions.emplace(partition, std::move(records));
    }
  }
  return partitions;
}

}  // namespace dataflow
}  // namespace pelton
