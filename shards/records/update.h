// Create dataflow records expressing the effects of UPDATE statements.

#ifndef SHARDS_RECORDS_UPDATE_H_
#define SHARDS_RECORDS_UPDATE_H_

#include <vector>

#include "absl/status/statusor.h"
#include "dataflow/record.h"
#include "shards/sqlast/ast.h"
#include "shards/state.h"

namespace shards {
namespace records {
namespace update {

absl::StatusOr<std::vector<dataflow::Record>> MakeRecords(
    const sqlast::Update &stmt, SharderState *state);

}  // namespace update
}  // namespace records
}  // namespace shards

#endif  // SHARDS_RECORDS_UPDATE_H_
