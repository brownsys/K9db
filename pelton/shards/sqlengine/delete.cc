// Delete statements sharding and rewriting.

#include "pelton/shards/sqlengine/delete.h"

#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace delete_ {

absl::StatusOr<mysql::SqlResult> Shard(const sqlast::Delete &stmt,
                                       SharderState *state,
                                       dataflow::DataFlowState *dataflow_state,
                                       bool update_flows) {
  perf::Start("Delete");
  const std::string &table_name = stmt.table_name();

  // Get the rows that are going to be deleted prior to deletion to use them
  // to update the dataflows.
  std::vector<dataflow::Record> records;
  if (update_flows) {
    MOVE_OR_RETURN(mysql::SqlResult result,
                   select::Shard(stmt.SelectDomain(), state, dataflow_state));
    records = result.Vectorize();
  }

  // Must transform the delete statement into one that is compatible with
  // the sharded schema.
  bool is_sharded = state->IsSharded(table_name);
  bool is_pii = state->IsPII(table_name);
  sqlast::Stringifier stringifier;
  mysql::SqlResult result;

  // Sharding scenarios.
  if (is_pii) {
    // Case 1: Table has PII.
    sqlast::ValueFinder value_finder(state->PkOfPII(table_name));

    // We are deleting a user, we should also delete their shard!
    auto [found, user_id] = stmt.Visit(&value_finder);
    if (!found) {
      return absl::InvalidArgumentError(
          "DELETE PII statement does not specify an exact user to delete!");
    }

    // Remove user shard.
    std::string shard = sqlengine::NameShard(table_name, user_id);
    state->connection_pool().RemoveShard(shard);
    state->RemoveUserFromShard(table_name, user_id);

    // Turn the delete statement back to a string, to delete relevant row in
    // PII table.
    std::string delete_str = stmt.Visit(&stringifier);
    result = state->connection_pool().ExecuteDefault(
        ConnectionPool::Operation::UPDATE, delete_str);

    // TODO(babman): Update dataflow after user has been deleted.
    // return absl::UnimplementedError("Dataflow not updated after a user
    // delete");

  } else if (!is_sharded && !is_pii) {
    // Case 2: Table does not have PII and is not sharded!
    std::string delete_str = stmt.Visit(&stringifier);
    result = state->connection_pool().ExecuteDefault(
        ConnectionPool::Operation::UPDATE, delete_str);

  } else {  // is_shared == true
    // Case 3: Table is sharded!
    // The table might be sharded according to different column/owners.
    // We must delete from all these different duplicates.
    for (const auto &info : state->GetShardingInformation(table_name)) {
      // Rename the table to match the sharded name.
      sqlast::Delete cloned = stmt;
      cloned.table_name() = info.sharded_table_name;

      // Find the value assigned to shard_by column in the where clause, and
      // remove it from the where clause.
      sqlast::ValueFinder value_finder(info.shard_by);
      auto [found, user_id] = cloned.Visit(&value_finder);
      if (found) {
        if (state->ShardExists(info.shard_kind, user_id)) {
          // Remove where condition on the shard by column, since it does not
          // exist in the sharded table.
          sqlast::ExpressionRemover expression_remover(info.shard_by);
          cloned.Visit(&expression_remover);

          // Execute statement directly against shard.
          std::string delete_str = cloned.Visit(&stringifier);
          result.Append(state->connection_pool().ExecuteShard(
              ConnectionPool::Operation::UPDATE, delete_str, info, user_id));
        }
      } else {
        // Execute statement against all shards of this kind.
        std::string delete_str = cloned.Visit(&stringifier);
        result.Append(state->connection_pool().ExecuteShards(
            ConnectionPool::Operation::UPDATE, delete_str, info,
            state->UsersOfShard(info.shard_kind)));
      }
    }
  }

  // Delete was successful, time to update dataflows.
  if (update_flows) {
    dataflow_state->ProcessRecords(table_name, records);
  }

  perf::End("Delete");
  return result;
}

}  // namespace delete_
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
