// SELECT statements sharding and rewriting.
#include "pelton/shards/sqlengine/select.h"

#include <string>

#include "pelton/util/perf.h"

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
  sqlast::Stringifier stringifier;
  // Table name to select from.
  const std::string &table_name = stmt.table_name();
  dataflow::SchemaRef schema = dataflow_state->GetTableSchema(table_name);

  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    // Case 1: table is not in any shard.
    std::string select_str = stmt.Visit(&stringifier);
    result = state->connection_pool().ExecuteDefault(
        ConnectionPool::Operation::QUERY, select_str, schema);

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
          std::string select_str = cloned.Visit(&stringifier);
          result.MakeInline();
          result.AppendDeduplicate(state->connection_pool().ExecuteShard(
              ConnectionPool::Operation::QUERY, select_str, info, user_id,
              schema));
        }
      } else {
        // Select from all the relevant shards.
        std::string select_str = cloned.Visit(&stringifier);
        result.MakeInline();
        result.AppendDeduplicate(state->connection_pool().ExecuteShards(
            ConnectionPool::Operation::QUERY, select_str, info,
            state->UsersOfShard(info.shard_kind), schema));
      }
    }
  }

  perf::End("Select");
  return result;
}

}  // namespace select
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
