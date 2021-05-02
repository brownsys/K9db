// INSERT statements sharding and rewriting.

#include "pelton/shards/sqlengine/insert.h"

#include <string>
#include <vector>

#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace insert {

namespace {

std::string Dequote(const std::string &st) {
  std::string s(st);
  s.erase(remove(s.begin(), s.end(), '\"'), s.end());
  s.erase(remove(s.begin(), s.end(), '\''), s.end());
  return s;
}

}  // namespace

absl::StatusOr<mysql::SqlResult> Shard(const sqlast::Insert &stmt,
                                       SharderState *state,
                                       dataflow::DataFlowState *dataflow_state,
                                       bool update_flows) {
  perf::Start("Insert");
  // Make sure table exists in the schema first.
  const std::string &table_name = stmt.table_name();
  if (!dataflow_state->HasFlowsFor(table_name)) {
    update_flows = false;
  }
  if (!state->Exists(table_name)) {
    return absl::InvalidArgumentError("Table does not exist!");
  }

  // Shard the insert statement so it is executable against the physical
  // sharded database.
  mysql::SqlResult result;

  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    // Case 1: table is not in any shard.
    // The insertion statement is unmodified.
    result = state->connection_pool().ExecuteDefault(&stmt);

  } else {  // is_sharded == true
    // Case 2: table is sharded!
    // Duplicate the value for every shard this table has.
    for (const ShardingInformation &info :
         state->GetShardingInformation(table_name)) {
      sqlast::Insert cloned = stmt;
      cloned.table_name() = info.sharded_table_name;
      // Find the value corresponding to the shard by column.
      std::string user_id;
      if (cloned.HasColumns()) {
        ASSIGN_OR_RETURN(user_id, cloned.RemoveValue(info.shard_by));
      } else {
        user_id = cloned.RemoveValue(info.shard_by_index);
      }
      user_id = Dequote(user_id);

      // TODO(babman): better to do this after user insert rather than user data
      //               insert.
      if (!state->ShardExists(info.shard_kind, user_id)) {
        for (auto *create_stmt : state->CreateShard(info.shard_kind, user_id)) {
          mysql::SqlResult tmp =
              state->connection_pool().ExecuteShard(create_stmt, info, user_id);
          if (!tmp.IsStatement() || !tmp.Success()) {
            return absl::InternalError("Could not created sharded table");
          }
        }
      }

      // Add the modified insert statement.
      result.Append(
          state->connection_pool().ExecuteShard(&cloned, info, user_id));
    }
  }

  // Insert was successful, time to update dataflows.
  // Turn inserted values into a record and process it via corresponding flows.
  if (update_flows) {
    std::vector<dataflow::Record> records;
    records.push_back(dataflow_state->CreateRecord(stmt));
    dataflow_state->ProcessRecords(stmt.table_name(), records);
  }

  perf::End("Insert");
  return result;
}

}  // namespace insert
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
