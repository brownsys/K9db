#ifndef PELTON_DATAFLOW_PARTITION_H_
#define PELTON_DATAFLOW_PARTITION_H_

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

namespace partition {
uint16_t GetPartition(const Key &key, const uint16_t &total_partitions);

// Partition given records
absl::flat_hash_map<uint16_t, std::vector<Record>> HashPartition(
    const std::vector<Record> &records, const std::vector<ColumnID> &cols,
    const uint16_t &total_partitions);
}  // namespace partition

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_PARTITION_H_
