// Create dataflow records expressing the effects of INSERT statements.

#ifndef PELTON_SHARDS_RECORDS_INSERT_H_
#define PELTON_SHARDS_RECORDS_INSERT_H_

#include <vector>

#include "absl/status/statusor.h"
#include "pelton/dataflow/record.h"
#include "pelton/shards/state.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace records {
namespace insert {

absl::StatusOr<std::vector<dataflow::Record>> MakeRecords(
    const sqlast::Insert &stmt, SharderState *state);

}  // namespace insert
}  // namespace records
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_RECORDS_INSERT_H_
