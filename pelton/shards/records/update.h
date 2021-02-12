// Create dataflow records expressing the effects of UPDATE statements.

#ifndef PELTON_SHARDS_RECORDS_UPDATE_H_
#define PELTON_SHARDS_RECORDS_UPDATE_H_

#include <vector>

#include "absl/status/statusor.h"
#include "pelton/dataflow/record.h"
#include "pelton/shards/state.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace records {
namespace update {

absl::StatusOr<std::vector<dataflow::Record>> MakeRecords(
    const sqlast::Update &stmt, SharderState *state);

}  // namespace update
}  // namespace records
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_RECORDS_UPDATE_H_
