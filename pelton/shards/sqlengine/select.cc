// SELECT statements sharding and rewriting.
#include "pelton/shards/sqlengine/select.h"

#include <memory>
#include <string>

#include "pelton/shards/sqlengine/index.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace select {

absl::StatusOr<mysql::SqlResult> Shard(
    const sqlast::Select &stmt, SharderState *state,
    dataflow::DataFlowState *dataflow_state) {
  perf::Start("Select");
  // Disqualifiy LIMIT and OFFSET queries.
  if (!stmt.SupportedByShards()) {
    return absl::InvalidArgumentError("Query contains unsupported features");
  }

  mysql::SqlResult result;
  // Table name to select from.
  const std::string &table_name = stmt.table_name();
  dataflow::SchemaRef schema = dataflow_state->GetTableSchema(table_name);

  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    // Case 1: table is not in any shard.
    result = state->connection_pool().ExecuteDefault(&stmt, schema);

  } else {  // is_sharded == true
    // Case 2: table is sharded.
    // We will query all the different ways of sharding the table (for
    // duplicates with many owners), de-duplication occurs later in the
    // pipeline.
    // When there are multiple ways of sharding the table (e.g. multiple infos),
    // that means that the table represents shared data between users.
    // E.g. direct messages between a sender and receiver.
    // Data is intentionally duplicated among these shards and must be
    // deduplicated before returning query result.
    for (const auto &info : state->GetShardingInformation(table_name)) {
      // Rename the table to match the sharded name.
      sqlast::Select cloned = stmt;
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
          result.MakeInline();
          result.AppendDeduplicate(state->connection_pool().ExecuteShard(
              &cloned, info, user_id, schema));
        }
      } else {
        // The select statement by itself does not obviously constraint a shard.
        // Try finding the shard(s) via secondary indices.
        ASSIGN_OR_RETURN(
            const auto &pair,
            index::LookupIndex(table_name, info.shard_by, stmt.GetWhereClause(),
                               state, dataflow_state));
        if (pair.first) {
          // Secondary index available for some constrainted column in stmt.
          result.MakeInline();
          result.AppendDeduplicate(state->connection_pool().ExecuteShards(
              &cloned, info, pair.second, schema));
        } else {
          // Secondary index unhelpful.
          // Select from all the relevant shards.
          result.MakeInline();
          result.AppendDeduplicate(state->connection_pool().ExecuteShards(
              &cloned, info, state->UsersOfShard(info.shard_kind), schema));
        }
      }
    }
  }

  if (!result.IsQuery()) {
    result =
        mysql::SqlResult{std::make_unique<mysql::InlinedSqlResult>(), schema};
  }

  perf::End("Select");
  return result;
}

}  // namespace select
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
