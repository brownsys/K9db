// Handling for Policy statements

#ifndef PELTON_SHARDS_SQLENGINE_POLICY_H_
#define PELTON_SHARDS_SQLENGINE_POLICY_H_

#include "absl/status/statusor.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace policy {

absl::StatusOr<sql::SqlResult> Shard(const sqlast::PolicyStatement &stmt,
                                     SharderState *state,
                                     dataflow::DataFlowState *dataflow_state);

}  // namespace policy
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_GDPR_H_
