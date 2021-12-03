// INSERT statements sharding and rewriting.

#include "pelton/shards/sqlengine/insert.h"

#include <string>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/connection.h"

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

absl::Status MaybeHandleOwningColumn(
  const sqlast::Insert &stmt,
  const sqlast::Insert &cloned,
  Connection* connection,
  bool *update_flows,
  sql::SqlResult *result,
  const ShardingInformation &info,
  const std::string &user_id) 
{
  auto &exec = connection->executor;
  shards::SharderState *state = connection->state->sharder_state();
  const sqlast::CreateTable &rel_schema = state->GetSchema(stmt.table_name());
  bool was_moved = false;
  for (auto &col : rel_schema.GetColumns()) {
    if (IsOwning(col)) {
      LOG(INFO) << "Found owning column";
      if (was_moved) {
        return absl::InvalidArgumentError("Two owning columns?");
      } else {
        was_moved = true;
        const sqlast::ColumnConstraint *constr = nullptr;
        for (auto &aconstr : col.GetConstraints()) {
          if (aconstr.type() == sqlast::ColumnConstraint::Type::FOREIGN_KEY) {
            if (constr == nullptr) {
              constr = &aconstr;
            } else {
              return absl::InvalidArgumentError("Too many foreign key constraints");
            }
          }
        }
        if (!constr) return absl::InvalidArgumentError("Expected to find a foreign key constraint");
        const std::string &target_table = constr->foreign_table();
        sqlast::Select select(
          target_table
        );
        auto binexp = std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::EQ);
        binexp->SetLeft(std::make_unique<sqlast::ColumnExpression>(constr->foreign_column()));
        // Handle if the insert statement does not set all columns (or in different order)
        if (cloned.GetColumns().size() != 0)
          return absl::InternalError("Unhandeled");
        size_t fk_idx =  rel_schema.ColumnIndex(col.column_name());
        if (!info.IsTransitive()) 
          --fk_idx; // this is to account for the dropped column in directly sharded tables. Might be a bit unstable.
        binexp->SetRight(std::make_unique<sqlast::LiteralExpression>(
          cloned.GetValue(fk_idx)
        ));
        select.SetWhereClause(
          std::move(binexp)
        );
        const sqlast::CreateTable schema = state->GetSchema(target_table);
        auto &sharding_info = state->GetShardingInformation(target_table);
        if (sharding_info.size() != 1)
          return absl::InternalError("Not implemented");
        sqlast::Insert insert(
          sharding_info.front().sharded_table_name
        );
        for (auto &col0 : schema.GetColumns()) {
          insert.AddColumn(col0.column_name());
          select.AddColumn(col0.column_name());
        }
        LOG(INFO) << "Doing lookup for " << schema.table_name();
        ASSIGN_OR_RETURN(sql::SqlResult &inner_result, select::Shard(select, connection, true));
        if (inner_result.IsQuery() && !inner_result.HasResultSet()) {
          LOG(INFO) << "Skipping value moving. Reason: The lookup has no results";
          continue;
        }
        auto result_set = inner_result.NextResultSet();
        auto iter = result_set->begin();
        if (iter == result_set->end()) {
          LOG(INFO) << "Skipping value moving. Reason: The result iter was empty";
          continue;
        }
        dataflow::Record &rec = *iter;
        LOG(INFO) << rec;
        uint64_t csize = schema.GetColumns().size();
        for (uint64_t i = 0; i < csize; i++) {
          LOG(INFO) << i << " : " << rec.GetValueString(i);
          insert.AddValue(rec.GetValue(i).GetSqlString());
        }
        if (++iter != result_set->end()) {
          return absl::InvalidArgumentError("Too many results?");
        }
        LOG(INFO) << "Inserting the moved value into " << insert.table_name() << " for " << user_id;
        result->Append(exec.ExecuteShard(&insert, info.shard_kind, user_id));
        LOG(INFO) << "Moving value done";
      }
    }
  }
  return absl::OkStatus();
}

absl::Status HandleShardForUser(
  const sqlast::Insert &stmt,
  const sqlast::Insert &cloned,
  Connection *connection,
  SharedLock *lock,
  bool *update_flows,
  sql::SqlResult *result,
  const ShardingInformation &info,
  const std::string &user_id)
{
  auto &exec = connection->executor;
  shards::SharderState *state = connection->state->sharder_state();

  // TODO(babman): better to do this after user insert rather than user data
  //               insert.
  if (!state->ShardExists(info.shard_kind, user_id)) {
    // Need to upgrade to an exclusive lock on sharder state here, as we
    // will modify the state.
    UniqueLock upgraded(std::move(*lock));
    if (!state->ShardExists(info.shard_kind, user_id)) {
      for (auto *create_stmt :
            state->CreateShard(info.shard_kind, user_id)) {
        sql::SqlResult tmp =
            exec.ExecuteShard(create_stmt, info.shard_kind, user_id);
        if (!tmp.IsStatement() || !tmp.Success()) {
          return absl::InternalError("Could not created sharded table");
        }
      }
    }
    *lock = SharedLock(std::move(upgraded));
  }

  // Add the modified insert statement.
  result->Append(exec.ExecuteShard(&cloned, info.shard_kind, user_id));
  LOG(INFO) << "Basic insert done";
  return MaybeHandleOwningColumn(stmt, cloned, connection, update_flows, result, info, user_id);
}

}  // namespace

absl::StatusOr<sql::SqlResult> Shard(const sqlast::Insert &stmt,
                                     Connection *connection, SharedLock *lock,
                                     bool update_flows) {
  perf::Start("Insert");
  shards::SharderState *state = connection->state->sharder_state();
  dataflow::DataFlowState *dataflow_state = connection->state->dataflow_state();

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

  auto &exec = connection->executor;
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    // Case 1: table is not in any shard.
    // The insertion statement is unmodified.
    LOG(INFO) << "Table " << table_name << " is not sharded.";
    result = exec.ExecuteDefault(&stmt);

  } else {  // is_sharded == true
    // Case 2: table is sharded!
    // Duplicate the value for every shard this table has.
    for (const ShardingInformation &info :
         state->GetShardingInformation(table_name)) {
      LOG(INFO) << "Handling shard " << info.sharded_table_name;
      sqlast::Insert cloned = stmt;
      cloned.table_name() = info.sharded_table_name;
      // Find the value corresponding to the shard by column.
      std::string user_id;
      if (cloned.HasColumns()) {
        if (info.IsTransitive()) {
          LOG(INFO) << "Table was found to be transitive and have columns";
          ASSIGN_OR_RETURN(user_id, cloned.GetValue(info.shard_by));
        } else {
          ASSIGN_OR_RETURN(user_id, cloned.RemoveValue(info.shard_by));
        }
      } else {
        if (info.IsTransitive()) {
          LOG(INFO) << "Table was found to be transitive";
          user_id = cloned.GetValue(info.shard_by_index);
        } else {
          user_id = cloned.RemoveValue(info.shard_by_index);
        }
      }
      LOG(INFO) << "user_id retrieved";
      if (absl::EqualsIgnoreCase(user_id, "NULL")) {
        return absl::InvalidArgumentError(info.shard_by + " cannot be NULL");
      }
      user_id = Dequote(user_id);
      LOG(INFO) << "user_id dequoted";

      // If the sharding is transitive, the user id should be resolved via the
      // secondary index of the target table.
      //
      // Checking the index used to only ever return a single result, because for each *column*   
      // there could only be one associated owner. Now with variable owners the index may return 
      // multiple owners *for the same colum*! Hence I refactored the shard handling code into
      // its own function and I'm calling it here either on the original user (else branch) or in a 
      // loop for each of the variable owners.
      if (info.IsTransitive()) {
        LOG(INFO) << "Looking up using " << info.next_index_name;
        ASSIGN_OR_RETURN(
            auto &lookup,
            index::LookupIndex(info.next_index_name, user_id, dataflow_state));
        if (lookup.size() < 1) {
          LOG(INFO) << "Unexpected lookup size " << lookup.size();
          return absl::InvalidArgumentError("Foreign Key Value does not exist");
        } else {
          LOG(INFO) << "Index lookup succeeded";
          for (auto &uid : lookup) {
            CHECK_STATUS(HandleShardForUser(stmt, cloned, connection, lock, &update_flows, &result, info, uid));
          }
        }
      } else {
        CHECK_STATUS(HandleShardForUser(stmt, cloned, connection, lock, &update_flows, &result, info, user_id));
      }
    }
  }

  // Insert was successful, time to update dataflows.
  // Turn inserted values into a record and process it via corresponding flows.
  if (update_flows) {
    std::vector<dataflow::Record> records;
    records.push_back(dataflow_state->CreateRecord(stmt));
    dataflow_state->ProcessRecords(stmt.table_name(), std::move(records));
  }

  perf::End("Insert");
  return result;
}

}  // namespace insert
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
