// Create dataflow records expressing the effects of INSERT statements.

#ifndef SHARDS_RECORDS_INSERT_H_
#define SHARDS_RECORDS_INSERT_H_

#include <vector>

#include "absl/status/statusor.h"
#include "dataflow/record.h"
#include "shards/sqlast/ast.h"
#include "shards/state.h"

namespace shards {
namespace records {
namespace insert {

absl::StatusOr<std::vector<dataflow::Record>> MakeRecords(
    const sqlast::Insert &stmt, SharderState *state);

}  // namespace insert
}  // namespace records
}  // namespace shards

#endif  // SHARDS_RECORDS_INSERT_H_
