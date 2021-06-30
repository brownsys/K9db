#ifndef PELTON_DATAFLOW_PARTITION_H_
#define PELTON_DATAFLOW_PARTITION_H_

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

// Partition given records
absl::flat_hash_map<uint16_t, std::vector<Record>> PartitionTrivial(
    std::vector<Record> records, std::vector<ColumnID> cols, uint16_t nthreads);

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_PARTITION_H_
