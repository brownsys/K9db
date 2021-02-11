// Create dataflow records expressing the effects of DELETE statements.

#ifndef SHARDS_RECORDS_DELETE_H_
#define SHARDS_RECORDS_DELETE_H_

#include <vector>

#include "absl/status/statusor.h"
#include "dataflow/record.h"
#include "shards/sqlast/ast.h"
#include "shards/state.h"

namespace shards {
namespace records {
namespace delete_ {

absl::StatusOr<std::vector<dataflow::Record>> MakeRecords(
    const sqlast::Delete &stmt, SharderState *state);

}  // namespace delete_
}  // namespace records
}  // namespace shards

#endif  // SHARDS_RECORDS_DELETE_H_
