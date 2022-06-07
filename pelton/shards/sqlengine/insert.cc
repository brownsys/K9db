// INSERT statements sharding and rewriting.

#include "pelton/shards/sqlengine/insert.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "pelton/connection.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace insert {

namespace {

// This is perhaps not the best way to resolve this but it works for the time
// being. We can optimize this later.
//
// The purpose of this function is to select from all the sharding information
// the one that corresponds to a variable owner table into which we are copying
// data
const ShardedTableName &VarownShardedTableName(
    const SharderState &state, const UnshardedTableName &target_table,
    const std::string &column_name, const UnshardedTableName &source_table) {
  for (auto &info : state.GetShardingInformation(target_table)) {
    if (info.next_column == column_name && info.next_table == source_table) {
      return info.sharded_table_name;
    }
  }
  // I really tried to make this work with abseil, but I was unable to make
  // `StatusOr` work with references
  throw std::runtime_error("Could not find sharded schema for table " +
                           target_table + " on column " + column_name);
}

absl::StatusOr<std::string> GetFKValueHelper(
    const sqlast::ColumnDefinition &col, const ShardingInformation &info,
    const sqlast::Insert &stmt, const sqlast::CreateTable &schema) {
  if (stmt.HasColumns()) {
    return stmt.GetValue(col.column_name());
  }
  size_t fk_idx = schema.ColumnIndex(col.column_name());
  if (!info.IsTransitive()) {
    if (info.shard_by_index < fk_idx) --fk_idx;
    CHECK_EQ(schema.GetColumns().size() - 1, stmt.GetValues().size());
  } else {
    CHECK_EQ(schema.GetColumns().size(), stmt.GetValues().size());
  }
  return stmt.GetValue(fk_idx);
}

std::unique_ptr<sqlast::BinaryExpression> MakePointSelectBinexp(
    const std::string &col, const std::string &col_val) {
  auto binexp =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::EQ);
  binexp->SetLeft(std::make_unique<sqlast::ColumnExpression>(col));
  binexp->SetRight(std::make_unique<sqlast::LiteralExpression>(col_val));
  return binexp;
}

absl::Status MaybeHandleOwningColumn(const sqlast::Insert &stmt,
                                     const sqlast::Insert &cloned,
                                     Connection *connection, bool *update_flows,
                                     sql::SqlResult *result,
                                     const ShardingInformation &info,
                                     const std::string &user_id) {
  auto &exec = connection->executor;
  shards::SharderState *state = connection->state->sharder_state();
  const sqlast::CreateTable &rel_schema = state->GetSchema(stmt.table_name());
  bool was_moved = false;
  for (auto &col : rel_schema.GetColumns()) {
    if (IsOwning(col)) {
      VLOG(1) << "Found owning column" << col;
      if (was_moved) {
        return absl::InvalidArgumentError("Two owning columns?");
      } else {
        was_moved = true;
        const sqlast::ColumnConstraint &constr = col.GetForeignKeyConstraint();
        ASSIGN_OR_RETURN(const auto &col_val,
                         GetFKValueHelper(col, info, cloned, rel_schema));
        VLOG(1) << "FK value found";

        // Short-circuit move if target row is already moved.
        const auto &info = state->GetShardingInformation(stmt.table_name());
        if (info.size() == 1) {
          const auto &stmt_table = stmt.table_name();
          const auto &col_name = col.column_name();
          const auto &shard_by = info.front().shard_by;
          if (state->HasIndexFor(stmt_table, col_name, shard_by)) {
            const auto &idx = state->IndexFlow(stmt_table, col_name, shard_by);
            ASSIGN_OR_RETURN(auto &lookup,
                             index::LookupIndex(idx, col_val, connection));
            bool already_in_shard = false;
            for (const auto &val : lookup) {
              if (val == user_id) {
                already_in_shard = true;
              }
            }
            if (already_in_shard) {
              VLOG(1) << "Skipping value moving. Reason: already moved";
              continue;
            }
          }
        }

        const std::string &target_table = constr.foreign_table();
        sqlast::Select select(target_table);
        select.SetWhereClause(
            MakePointSelectBinexp(constr.foreign_column(), col_val));
        const sqlast::CreateTable schema = state->GetSchema(target_table);
        const ShardedTableName &sharded_table = VarownShardedTableName(
            *state, target_table, col.column_name(), stmt.table_name());
        sqlast::Insert insert(sharded_table, false);
        for (auto &col0 : schema.GetColumns()) {
          insert.AddColumn(col0.column_name());
          select.AddColumn(col0.column_name());
        }
        bool needs_default_db_delete = false;
        ASSIGN_OR_RETURN(
            sql::SqlResult & inner_result,
            select::Shard(select, connection, true, &needs_default_db_delete));
        CHECK(inner_result.IsQuery());
        if (inner_result.empty()) {
          VLOG(1) << "Skipping value moving. Reason: The lookup has no results";
          continue;
        }
        auto &result_set = inner_result.ResultSets().front();
        if (inner_result.ResultSets().size() > 1 || result_set.size() > 1) {
          LOG(WARNING) << "Too many results for " << schema.table_name()
                       << " during value move";
        }
        const dataflow::Record &rec = result_set.Vec().front();
        uint64_t csize = schema.GetColumns().size();
        if (csize != result_set.schema().size())
          LOG(FATAL) << csize << " != " << result_set.schema().size();
        for (uint64_t i = 0; i < csize; i++) {
          auto v = rec.GetValue(i).GetSqlString();
          insert.AddValue(v);
        }
        result->Append(exec.Shard(&insert, user_id), false);
        if (needs_default_db_delete) {
          sqlast::Delete del(target_table);
          del.SetWhereClause(
              MakePointSelectBinexp(constr.foreign_column(), col_val));
          exec.Default(&del);
        }
      }
    }
  }
  return absl::OkStatus();
}

absl::Status HandleShardForUser(const sqlast::Insert &stmt,
                                const sqlast::Insert &cloned,
                                Connection *connection, SharedLock *lock,
                                bool *update_flows, sql::SqlResult *result,
                                const ShardingInformation &info,
                                const std::string &user_id,
                                const dataflow::SchemaRef &schema,
                                const int aug_index) {
  auto &exec = connection->executor;
  shards::SharderState *state = connection->state->sharder_state();

  // TODO(babman): better to do this after user insert rather than user data
  //               insert.
  if (!state->ShardExists(info.shard_kind, user_id)) {
    // Need to upgrade to an exclusive lock on sharder state here, as we
    // will modify the state.
    UniqueLock upgraded(std::move(*lock));
    if (!state->ShardExists(info.shard_kind, user_id)) {
      for (auto *create_stmt : state->CreateShard(info.shard_kind, user_id)) {
        if (!exec.Shard(create_stmt, user_id).Success()) {
          return absl::InternalError("Could not created sharded table");
        }
      }
    }
    *lock = SharedLock(std::move(upgraded));
  }
  // Add the modified insert statement.
  result->Append(exec.Shard(&cloned, user_id, schema, aug_index), true);
  return MaybeHandleOwningColumn(stmt, cloned, connection, update_flows, result,
                                 info, user_id);
}

}  // namespace

absl::StatusOr<sql::SqlResult> Shard(const sqlast::Insert &stmt,
                                     Connection *connection, SharedLock *lock,
                                     bool update_flows) {
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

  // For replace insert statements, we need to get the old records.
  bool returning = update_flows && stmt.replace();
  dataflow::SchemaRef schema = dataflow_state->GetTableSchema(table_name);

  // Shard the insert statement so it is executable against the physical
  // sharded database.
  sql::SqlResult result(static_cast<int>(0));
  if (returning) {
    result = sql::SqlResult(schema);
  }

  auto &exec = connection->executor;
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    // Case 1: table is not in any shard.
    // The insertion statement is unmodified.
    if (returning) {
      sqlast::Insert cloned = stmt.MakeReturning();
      result = exec.Default(&cloned, schema);
    } else {
      result = exec.Default(&stmt);
    }

  } else {  // is_sharded == true
    // Case 2: table is sharded!
    // Duplicate the value for every shard this table has.
    for (const ShardingInformation &info :
         state->GetShardingInformation(table_name)) {
      sqlast::Insert cloned = returning ? stmt.MakeReturning() : stmt;
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
        LOG(WARNING) << info.shard_by << " was NULL";
        LOG(WARNING) << "This could indicate a bug and may lead to data loss!";
        continue;
      }
      user_id = Dequote(user_id);

      // Augment user_id for returning replaces.
      int aug_index = -1;
      if (returning && !info.IsTransitive()) {
        aug_index = info.shard_by_index;
      }

      // If the sharding is transitive, the user id should be resolved via the
      // secondary index of the target table.
      //
      // Checking the index used to only ever return a single result, because
      // for each *column* there could only be one associated owner. Now with
      // variable owners the index may return multiple owners *for the same
      // colum*! Hence I refactored the shard handling code into its own
      // function and I'm calling it here either on the original user (else
      // branch) or in a loop for each of the variable owners.
      if (info.IsTransitive()) {
        ASSIGN_OR_RETURN(auto &lookup, index::LookupIndex(info.next_index_name,
                                                          user_id, connection));
        if (lookup.size() < 1) {
          if (info.is_varowned()) {
            // In case the table is variably owned this pointed-to resource may
            // have been inserted before the relationship record is inserted. In
            // that case we insert into the default table instead and skip the
            // sharded insert.

            result = exec.Default(&stmt);
            continue;
          } else {
            return absl::InvalidArgumentError(
                "Foreign Key Value does not exist");
          }
        }

        for (auto &uid : lookup) {
          if (!absl::EqualsIgnoreCase(uid, "NULL")) {
            CHECK_STATUS(HandleShardForUser(stmt, cloned, connection, lock,
                                            &update_flows, &result, info, uid,
                                            schema, aug_index));
          }
        }
      } else {
        CHECK_STATUS(HandleShardForUser(stmt, cloned, connection, lock,
                                        &update_flows, &result, info, user_id,
                                        schema, aug_index));
      }
    }
  }

  // Insert was successful, time to update dataflows.
  // Turn inserted values into a record and process it via corresponding flows.
  if (update_flows) {
    std::vector<dataflow::Record> records;
    if (returning) {
      records = result.ResultSets().at(0).Vec();
      result = sql::SqlResult(1 + records.size());
    }
    records.push_back(dataflow_state->CreateRecord(stmt));
    dataflow_state->ProcessRecords(stmt.table_name(), std::move(records));
  }

  return result;
}

}  // namespace insert
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
