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

absl::Status Shard(const sqlast::Insert &stmt, SharderState *state,
                   dataflow::DataFlowState *dataflow_state,
                   const OutputChannel &output, bool update_flows) {
  perf::Start("Insert");
  // Make sure table exists in the schema first.
  const std::string &table_name = stmt.table_name();
  if (!state->Exists(table_name)) {
    return absl::InvalidArgumentError("Table does not exist!");
  }

  // Shard the insert statement so it is executable against the physical
  // sharded database.
  sqlast::Stringifier stringifier;

  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    // Case 1: table is not in any shard.
    // The insertion statement is unmodified.
    std::string insert_str = stmt.Visit(&stringifier);
    CHECK_STATUS(state->connection_pool().ExecuteDefault(insert_str, output));

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

      // TODO(babman): better to do this after user insert rather than user data
      //               insert.
      if (!state->ShardExists(info.shard_kind, user_id)) {
        for (auto create_stmt : state->CreateShard(info.shard_kind, user_id)) {
          CHECK_STATUS(state->connection_pool().ExecuteShard(create_stmt, info,
                                                             user_id, output));
        }
      }

      // Add the modified insert statement.
      std::string insert_str = cloned.Visit(&stringifier);
      CHECK_STATUS(state->connection_pool().ExecuteShard(insert_str, info,
                                                         user_id, output));
    }
  }

  // Insert was successful, time to update dataflows.
  // Turn inserted values into a record and process it via corresponding flows.
  if (update_flows) {
    std::vector<RawRecord> records;
    records.emplace_back(table_name, stmt.GetValues(), stmt.GetColumns(), true);
    dataflow_state->ProcessRawRecords(records);
  }

  perf::End("Insert");
  return absl::OkStatus();
}

}  // namespace insert
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
