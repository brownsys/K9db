#include "pelton/dataflow/partition.h"

#include <stdlib.h>

#include <utility>

namespace pelton {
namespace dataflow {

namespace partition {

// Utility function
inline uint16_t ModuloHash(const size_t hash_value,
                           const uint16_t total_partitions) {
  return (uint16_t)(hash_value % (size_t)total_partitions);
}

uint16_t GetPartition(const Key &key, const uint16_t total_partitions) {
  return ModuloHash(key.Hash(), total_partitions);
}

absl::flat_hash_map<uint16_t, std::vector<Record>> HashPartition(
    const std::vector<Record> &records, const std::vector<ColumnID> &cols,
    const uint16_t total_partitions) {
  absl::flat_hash_map<uint16_t, std::vector<Record>> partitions;
  for (const Record &record : records) {
    uint16_t partition = ModuloHash(record.Hash(cols), total_partitions);
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
