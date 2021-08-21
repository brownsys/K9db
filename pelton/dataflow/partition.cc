#include "pelton/dataflow/partition.h"

#include <stdlib.h>

#include <utility>

namespace pelton {
namespace dataflow {

namespace partition {
uint16_t GetPartition(const Key &key, const uint16_t &total_partitions) {
  return (uint16_t)(key.Hash() % (size_t)total_partitions);
}

absl::flat_hash_map<uint16_t, std::vector<Record>> HashPartition(
    const std::vector<Record> &records, const std::vector<ColumnID> &cols,
    const uint16_t &total_partitions) {
  absl::flat_hash_map<uint16_t, std::vector<Record>> partitions;
  for (const Record &record : records) {
    size_t hash = record.Hash(cols);
    uint16_t partition = (uint16_t)(hash % (size_t)total_partitions);
    if (partitions.contains(partition)) {
      partitions.at(partition).push_back(record.Copy());
    } else {
      std::vector<Record> temp;
      temp.push_back(record.Copy());
      partitions.emplace(partition, std::move(temp));
    }
  }
  return partitions;
}
}  // namespace partition

}  // namespace dataflow
}  // namespace pelton
