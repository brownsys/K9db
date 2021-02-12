// Create dataflow records expressing the effects of DELETE statements.

#ifndef PELTON_SHARDS_RECORDS_DELETE_H_
#define PELTON_SHARDS_RECORDS_DELETE_H_

#include <vector>

#include "absl/status/statusor.h"
#include "pelton/dataflow/record.h"
#include "pelton/shards/state.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace records {
namespace delete_ {

absl::StatusOr<std::vector<dataflow::Record>> MakeRecords(
    const sqlast::Delete &stmt, SharderState *state);

}  // namespace delete_
}  // namespace records
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_RECORDS_DELETE_H_
