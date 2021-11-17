// INSERT statements sharding and rewriting.

#include "pelton/shards/sqlengine/insert.h"

#include <string>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "pelton/shards/sqlengine/index.h"
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

absl::StatusOr<sql::SqlResult> Shard(const sqlast::Insert &stmt,
                                     Connection *connection,
                                     bool update_flows) {
  perf::Start("Insert");
  dataflow::DataFlowState *dataflow_state =
      connection->pelton_state->GetDataFlowState();
  shards::SharderState *state = connection->pelton_state->GetSharderState();
  SharderStateLock state_lock = state->LockShared();

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
  sql::SqlResult result = sql::SqlResult(0);

  auto &exec = connection->executor_;
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    // Case 1: table is not in any shard.
    // The insertion statement is unmodified.
    result = exec.ExecuteDefault(&stmt);

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
        if (info.IsTransitive()) {
          ASSIGN_OR_RETURN(user_id, cloned.GetValue(info.shard_by));
        } else {
          ASSIGN_OR_RETURN(user_id, cloned.RemoveValue(info.shard_by));
        }
      } else {
        if (info.IsTransitive()) {
          user_id = cloned.GetValue(info.shard_by_index);
        } else {
          user_id = cloned.RemoveValue(info.shard_by_index);
        }
      }
      if (absl::EqualsIgnoreCase(user_id, "NULL")) {
        return absl::InvalidArgumentError(info.shard_by + " cannot be NULL");
      }
      user_id = Dequote(user_id);

      // If the sharding is transitive, the user id should be resolved via the
      // secondary index of the target table.
      if (info.IsTransitive()) {
        ASSIGN_OR_RETURN(
            auto &lookup,
            index::LookupIndex(info.next_index_name, user_id, dataflow_state));
        if (lookup.size() == 1) {
          user_id = std::move(*lookup.cbegin());
        } else {
          return absl::InvalidArgumentError("Foreign Key Value does not exist");
        }
      }

      // TODO(babman): better to do this after user insert rather than user data
      //               insert.
      if (!state->ShardExists(info.shard_kind, user_id)) {
        // Need to upgrade to an exclusive lock on sharder state here, as we
        // will modify the state.
        state_lock = state->LockUpgrade(std::move(state_lock));
        for (auto *create_stmt : state->CreateShard(info.shard_kind, user_id)) {
          sql::SqlResult tmp =
              exec.ExecuteShard(create_stmt, info.shard_kind, user_id);
          if (!tmp.IsStatement() || !tmp.Success()) {
            return absl::InternalError("Could not created sharded table");
          }
        }
      }

      // Add the modified insert statement.
      result.Append(exec.ExecuteShard(&cloned, info.shard_kind, user_id));
    }
  }

  state_lock.Unlock();

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
