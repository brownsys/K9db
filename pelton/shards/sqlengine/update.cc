// UPDATE statements sharding and rewriting.
#include "pelton/shards/sqlengine/update.h"

#include <string>

#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace update {

absl::Status Shard(const sqlast::Update &stmt, SharderState *state,
                   dataflow::DataFlowState *dataflow_state,
                   const OutputChannel &output) {
  sqlast::Stringifier stringifier;

  // Table name to select from.
  const std::string &table_name = stmt.table_name();

  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    // Case 1: table is not in any shard.
    std::string update_str = stmt.Visit(&stringifier);
    return state->connection_pool().ExecuteDefault(update_str, output);

  } else {  // is_sharded == true
    // Case 2: table is sharded.
    // The table might be sharded according to different column/owners.
    // We must update all these different duplicates.
    for (const auto &info : state->GetShardingInformation(table_name)) {
      if (stmt.AssignsTo(info.shard_by)) {
        return absl::InvalidArgumentError("Update cannot modify owner column");
      }

      // Rename the table to match the sharded name.
      sqlast::Update cloned = stmt;
      cloned.table_name() = info.sharded_table_name;

      // Find the value assigned to shard_by column in the where clause, and
      // remove it from the where clause.
      sqlast::ValueFinder value_finder(info.shard_by);
      auto [found, user_id] = cloned.Visit(&value_finder);
      if (found) {
        // Remove where condition on the shard by column, since it does not
        // exist in the sharded table.
        sqlast::ExpressionRemover expression_remover(info.shard_by);
        cloned.Visit(&expression_remover);

        // Execute statement directly against shard.
        std::string update_str = cloned.Visit(&stringifier);
        CHECK_STATUS(state->connection_pool().ExecuteShard(
            update_str, info.shard_kind, user_id, output));
      } else {
        // Update against the relevant shards.
        std::string update_str = cloned.Visit(&stringifier);
        CHECK_STATUS(state->connection_pool().ExecuteShard(
            update_str, info.shard_kind, state->UsersOfShard(info.shard_kind),
            output));
      }
    }

    return absl::OkStatus();
  }
}

}  // namespace update
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
