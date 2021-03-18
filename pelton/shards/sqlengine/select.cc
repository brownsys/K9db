// SELECT statements sharding and rewriting.
#include "pelton/shards/sqlengine/select.h"

#include <string>

#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace select {

absl::Status Shard(const sqlast::Select &stmt, SharderState *state,
                   dataflow::DataFlowState *dataflow_state,
                   const OutputChannel &output) {
  sqlast::Stringifier stringifier;

  // Table name to select from.
  const std::string &table_name = stmt.table_name();

  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    // Case 1: table is not in any shard.
    std::string select_str = stmt.Visit(&stringifier);
    return state->connection_pool().ExecuteDefault(select_str, output);

  } else {  // is_sharded == true
    // Case 2: table is sharded.
    // We will query all the different ways of sharding the table (for
    // duplicates with many owners), de-duplication occurs later in the
    // pipeline.
    // TODO(babman): put back deduplication code here.
    for (const auto &info : state->GetShardingInformation(table_name)) {
      // Rename the table to match the sharded name.
      sqlast::Select cloned = stmt;
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
        std::string select_str = cloned.Visit(&stringifier);
        CHECK_STATUS(state->connection_pool().ExecuteShard(
            select_str, info.shard_kind, user_id, output));
      } else {
        // Select from all the relevant shards.
        std::string select_str = cloned.Visit(&stringifier);
        CHECK_STATUS(state->connection_pool().ExecuteShard(
            select_str, info.shard_kind, state->UsersOfShard(info.shard_kind),
            output));
      }
    }

    return absl::OkStatus();
  }
}

}  // namespace select
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
