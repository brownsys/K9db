// Handling for Policy statements
#include "pelton/shards/sqlengine/policy.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/shards/types.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace policy {

absl::StatusOr<sql::SqlResult> Shard(const sqlast::PolicyStatement &stmt,
                                     SharderState *state,
                                     dataflow::DataFlowState *dataflow_state) {
  // FIXME: here, we're converting between what the server application thinks of
  // users as (probably a tuple of table name and some value from the primary
  // key column of that table, ie: (patients, "paul")) and what Pelton thinks of
  // users as (a ShardKind and a UserID). In general, these two could be very
  // different, but we get lucky because they are the same for now. If this
  // changes, we'll need to add conversion functions here.
  ShardKind shard_kind = stmt.table_name();
  UserId user_id = stmt.primary_key();
  
  std::unordered_set<std::string> allowed_purposes(stmt.purpose_names().begin(), stmt.purpose_names().end());

  state->SetPolicyInformation(shard_kind, user_id, PolicyInformation(allowed_purposes));

  // FIXME: what do we put in this result?
  return sql::SqlResult();
}

} // namespace policy
} // namespace sqlengine
} // namespace shards
} // namespace pelton
