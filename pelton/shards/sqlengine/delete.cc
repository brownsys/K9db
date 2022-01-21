// Delete statements sharding and rewriting.

#include "pelton/shards/sqlengine/delete.h"

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace delete_ {

namespace {

sql::SqlResult HandleOWNINGColumn(const UnshardedTableName &table_name,
                                  const UnshardedTableName &sharded_table_name,
                                  const std::string &user_id,
                                  const std::string &shard_kind,
                                  const dataflow::Record &delete_result,
                                  Connection *connection) {
  const auto &state = *connection->state->sharder_state();
  auto &exec = connection->executor;

  sql::SqlResult result(static_cast<int>(0));
  for (const auto &col : state.GetSchema(table_name).GetColumns()) {
    if (IsOwning(col)) {
      const sqlast::ColumnConstraint *fk_constr;
      for (const auto &constr : col.GetConstraints()) {
        if (constr.type() == sqlast::ColumnConstraint::Type::FOREIGN_KEY) {
          fk_constr = &constr;
        }
      }
      if (!fk_constr) {
        LOG(FATAL) << "Expected foreign key constraint";
      }
      const auto &foreign_table = fk_constr->foreign_table();
      const auto &infos =
          state.GetShardingInformationFor(foreign_table, shard_kind);
      for (const auto info : infos) {
        if (info->next_table != table_name) {
          continue;
        }

        sqlast::Delete del(info->sharded_table_name);
        auto where = std::make_unique<sqlast::BinaryExpression>(
            sqlast::Expression::Type::EQ);
        where->SetLeft(std::make_unique<sqlast::ColumnExpression>(
            fk_constr->foreign_column()));
        std::cerr << "Is it here?" << std::endl;
        where->SetRight(std::make_unique<sqlast::LiteralExpression>(
            delete_result
                .GetValue(delete_result.schema().IndexOf(col.column_name()))
                .GetSqlString()));
        std::cerr << "No";
        del.SetWhereClause(std::move(where));
        result.Append(exec.Shard(&del, shard_kind, user_id), true);
      }
    }
  }
  return result;
}

}  // namespace

absl::StatusOr<sql::SqlResult> Shard(const sqlast::Delete &stmt,
                                     Connection *connection, bool synchronize,
                                     bool update_flows) {
  connection->perf->Start("Delete");
  const std::string &table_name = stmt.table_name();
  LOG(INFO) << "Delete statement started";

  // We only read from state.
  shards::SharderState *state = connection->state->sharder_state();
  dataflow::DataFlowState *dataflow_state = connection->state->dataflow_state();

  std::vector<dataflow::Record> records;

  SharedLock lock;
  if (synchronize) {
    lock = state->ReaderLock();
  }

  // If no flows read from this table, we do not need to do anything
  // to update them.
  if (!dataflow_state->HasFlowsFor(table_name)) {
    update_flows = false;
  }

  // Get the rows that are going to be deleted prior to deletion to use them
  // to update the dataflows.
  dataflow::SchemaRef schema = dataflow_state->GetTableSchema(table_name);

  // Must transform the delete statement into one that is compatible with
  // the sharded schema.
  sql::SqlResult result(static_cast<int>(0));

  // Sharding scenarios.
  auto &exec = connection->executor;
  bool is_sharded = state->IsSharded(table_name);
  if (is_sharded) {  // is_shared == true
    // Case 3: Table is sharded!
    // The table might be sharded according to different column/owners.
    // We must delete from all these different duplicates.
    for (const auto &info : state->GetShardingInformation(table_name)) {
      // Rename the table to match the sharded name.
      sqlast::Delete cloned = stmt.MakeReturning();
      cloned.table_name() = info.sharded_table_name;

      // Figure out if we need to augment the (returning) result set with the
      // shard_by column.
      const std::string &shard_kind = info.shard_kind;
      int aug_index = -1;
      if (!info.IsTransitive()) {
        aug_index = info.shard_by_index;
      }

      // Find the value assigned to shard_by column in the where clause, and
      // remove it from the where clause.
      sqlast::ValueFinder value_finder(info.shard_by);
      auto [found, user_id] = cloned.Visit(&value_finder);
      if (found) {
        if (info.IsTransitive()) {
          // Transitive sharding: look up via index.
          ASSIGN_OR_RETURN(
              auto &lookup,
              index::LookupIndex(info.next_index_name, user_id, connection));
          for (auto &uid : lookup) {
            // Execute statement directly against shard.
            auto res = exec.Shard(&cloned, shard_kind, uid, schema, aug_index);
            CHECK_EQ(res.ResultSets().size(), 1);
            auto these_records = res.ResultSets().front().Vec();
            result.Append(sql::SqlResult(these_records.size()), true);
            for (auto &rec : these_records) {
              result.Append(
                  HandleOWNINGColumn(table_name, info.sharded_table_name, uid,
                                     shard_kind, rec, connection),
                  true);
              records.push_back(std::move(rec));
            }
          }
        } else if (state->ShardExists(info.shard_kind, user_id)) {
          // Remove where condition on the shard by column, since it does not
          // exist in the sharded table.
          sqlast::ExpressionRemover expression_remover(info.shard_by);
          cloned.Visit(&expression_remover);
          // Execute statement directly against shard.
          auto res =
              exec.Shard(&cloned, shard_kind, user_id, schema, aug_index);
          CHECK_EQ(res.ResultSets().size(), 1);
          auto these_records = res.ResultSets().front().Vec();
          result.Append(sql::SqlResult(these_records.size()), true);
          for (auto &rec : these_records) {
            result.Append(
                HandleOWNINGColumn(table_name, info.sharded_table_name, user_id,
                                   shard_kind, rec, connection),
                true);
            records.push_back(std::move(rec));
          }
        }

      } else {
        // The delete statement by itself does not obviously constraint a
        // shard. Try finding the shard(s) via secondary indices.
        ASSIGN_OR_RETURN(const auto &pair,
                         index::LookupIndex(table_name, info.shard_by,
                                            stmt.GetWhereClause(), connection));
        if (pair.first) {
          // Secondary index available for some constrainted column in stmt.
          auto res =
              exec.Shards(&cloned, shard_kind, pair.second, schema, aug_index);
          CHECK_EQ(res.ResultSets().size(), 1);
          auto these_records = res.ResultSets().front().Vec();
          result.Append(sql::SqlResult(these_records.size()), true);
          for (auto &rec : these_records) {
            result.Append(
                HandleOWNINGColumn(table_name, info.sharded_table_name, user_id,
                                   shard_kind, rec, connection),
                true);
            records.push_back(std::move(rec));
          }
        } else {
          // Secondary index unhelpful.
          // Execute statement against all shards of this kind.
          const auto &user_ids = state->UsersOfShard(shard_kind);
          auto res =
              exec.Shards(&cloned, shard_kind, user_ids, schema, aug_index);
          if (user_ids.size() > 0) {
            LOG(WARNING) << "Perf Warning: Delete over all shards " << stmt;
          }
          CHECK_EQ(res.ResultSets().size(), 1);
          auto these_records = res.ResultSets().front().Vec();
          result.Append(sql::SqlResult(these_records.size()), true);
          for (auto &rec : these_records) {
            auto id = rec.GetValueString(rec.schema().IndexOf(info.shard_by));
            result.Append(
                HandleOWNINGColumn(table_name, info.sharded_table_name, id,
                                   shard_kind, rec, connection),
                true);
            records.push_back(std::move(rec));
          }
        }
      }
    }
  } else {
    // Case 2: Table is not sharded.
    if (update_flows) {
      sqlast::Delete cloned = stmt.MakeReturning();
      auto res = exec.Default(&cloned, schema);
      CHECK_EQ(res.ResultSets().size(), 1);
      records = res.ResultSets().front().Vec();
      result.Append(sql::SqlResult(records.size()), true);
    } else {
      result = exec.Default(&stmt);
    }
  }

  // Delete was successful, time to update dataflows.
  if (update_flows) {
    dataflow_state->ProcessRecords(table_name, std::move(records));
  }

  connection->perf->End("Delete");
  return result;
}

}  // namespace delete_
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
