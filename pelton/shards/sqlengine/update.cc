// UPDATE statements sharding and rewriting.
#include "pelton/shards/sqlengine/update.h"

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/sqlengine/insert.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace update {

namespace {

int IndexOfColumn(const std::string &col_name,
                  const sqlast::CreateTable &table_schema) {
  for (size_t i = 0; i < table_schema.GetColumns().size(); i++) {
    const sqlast::ColumnDefinition &col = table_schema.GetColumns().at(i);
    if (col.column_name() == col_name) {
      return i;
    }
  }
  return -1;
}

// Apply the update statement to every record in records, using table_schema
// to resolve order of columns.
absl::Status UpdateRecords(std::vector<dataflow::Record> *records,
                           const sqlast::Update &stmt,
                           const sqlast::CreateTable &table_schema) {
  // What is being updated.
  const std::vector<std::string> &updated_cols = stmt.GetColumns();
  const std::vector<std::string> &updated_vals = stmt.GetValues();

  // Loop over records and apply update.
  size_t size = records->size();
  for (size_t i = 0; i < size; i++) {
    dataflow::Record record = records->at(i).Copy();
    record.SetPositive(true);
    for (size_t c = 0; c < updated_cols.size(); c++) {
      const std::string &col_name = updated_cols.at(c);
      int val_index = IndexOfColumn(col_name, table_schema);
      if (val_index < 0) {
        return absl::InvalidArgumentError("Unrecognized column " + col_name);
      }
      record.SetValue(updated_vals.at(c), val_index);
    }
    records->push_back(std::move(record));
  }

  return absl::OkStatus();
}

// Returns true if the update statement might change the shard of the target
// rows.
bool ModifiesShardBy(const sqlast::Update &stmt, SharderState *state) {
  for (const auto &info : state->GetShardingInformation(stmt.table_name())) {
    if (stmt.AssignsTo(info.shard_by)) {
      return true;
    }
  }
  return false;
}

// Creates a corresponding Insert statement for the given record, using given
// table_schema to resolve types of values.
sqlast::Insert InsertRecord(const dataflow::Record &record,
                            const sqlast::CreateTable &table_schema) {
  sqlast::Insert stmt{table_schema.table_name()};
  for (size_t i = 0; i < table_schema.GetColumns().size(); i++) {
    sqlast::ColumnDefinition::Type type =
        table_schema.GetColumns().at(i).column_type();
    switch (type) {
      case sqlast::ColumnDefinition::Type::UINT:
      case sqlast::ColumnDefinition::Type::INT:
        stmt.AddValue(record.GetValueString(i));
        break;
      case sqlast::ColumnDefinition::Type::TEXT:
      case sqlast::ColumnDefinition::Type::DATETIME:
        // TODO(babman): This can be improved later.
        // Values acquired from selecting from the database do not have an
        // enclosing ', while those we get from the ANTLR-parsed queries do
        // (e.g. the value set by an UPDATE statement).
        if (record.GetString(i)[0] != '\'') {
          stmt.AddValue("\'" + record.GetString(i) + "\'");
        } else {
          stmt.AddValue(record.GetString(i));
        }
        break;
    }
  }
  return stmt;
}

}  // namespace

absl::StatusOr<mysql::SqlResult> Shard(
    const sqlast::Update &stmt, SharderState *state,
    dataflow::DataFlowState *dataflow_state) {
  perf::Start("Update");

  // Table name to select from.
  const std::string &table_name = stmt.table_name();
  bool update_flows = dataflow_state->HasFlowsFor(table_name);

  // Get the rows that are going to be deleted prior to deletion to use them
  // to update the dataflows.
  std::vector<dataflow::Record> records;
  size_t old_records_size = 0;
  if (update_flows) {
    MOVE_OR_RETURN(mysql::SqlResult domain_result,
                   select::Shard(stmt.SelectDomain(), state, dataflow_state));
    records = domain_result.Vectorize();
    old_records_size = records.size();
    CHECK_STATUS(UpdateRecords(&records, stmt, state->GetSchema(table_name)));
  }

  mysql::SqlResult result;
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    // Case 1: table is not in any shard.
    result = state->connection_pool().ExecuteDefault(&stmt);

  } else {  // is_sharded == true
    // Case 2: table is sharded.
    if (ModifiesShardBy(stmt, state)) {
      // The update statement might move the rows from one shard to another.
      // We can only perform this update by splitting it into a DELETE-INSERT
      // pair.
      MOVE_OR_RETURN(
          mysql::SqlResult tmp,
          delete_::Shard(stmt.DeleteDomain(), state, dataflow_state, false));
      result.MakeInline();
      result.Append(std::move(tmp));

      // Insert updated records.
      for (size_t i = old_records_size; i < records.size(); i++) {
        sqlast::Insert insert_stmt =
            InsertRecord(records.at(i), state->GetSchema(table_name));
        MOVE_OR_RETURN(
            tmp, insert::Shard(insert_stmt, state, dataflow_state, false));
        result.MakeInline();
        result.Append(std::move(tmp));
      }

    } else {
      // The table might be sharded according to different column/owners.
      // We must update all these different duplicates.
      for (const auto &info : state->GetShardingInformation(table_name)) {
        // Rename the table to match the sharded name.
        sqlast::Update cloned = stmt;
        cloned.table_name() = info.sharded_table_name;

        // Find the value assigned to shard_by column in the where clause, and
        // remove it from the where clause.
        sqlast::ValueFinder value_finder(info.shard_by);
        auto [found, user_id] = cloned.Visit(&value_finder);
        if (found) {
          if (info.IsTransitive()) {
            // Transitive sharding: look up via index.
            ASSIGN_OR_RETURN(auto &lookup,
                             index::LookupIndex(info.next_index_name, user_id,
                                                dataflow_state));
            if (lookup.size() == 1) {
              user_id = std::move(*lookup.cbegin());
              // Execute statement directly against shard.
              result.MakeInline();
              result.Append(state->connection_pool().ExecuteShard(&cloned, info,
                                                                  user_id));
            }
          } else if (state->ShardExists(info.shard_kind, user_id)) {
            // Remove where condition on the shard by column, since it does
            // not exist in the sharded table.
            sqlast::ExpressionRemover expression_remover(info.shard_by);
            cloned.Visit(&expression_remover);
            // Execute statement directly against shard.
            result.MakeInline();
            result.Append(
                state->connection_pool().ExecuteShard(&cloned, info, user_id));
          }

        } else if (update_flows) {
          // We already have the data we need to delete, we can use it to get an
          // accurate enumeration of shards to execute this one.
          std::unordered_set<UserId> shards;
          for (const dataflow::Record &record : records) {
            std::string val = record.GetValueString(info.shard_by_index);
            if (info.IsTransitive()) {
              // Transitive sharding: look up via index.
              ASSIGN_OR_RETURN(auto &lookup,
                               index::LookupIndex(info.next_index_name, user_id,
                                                  dataflow_state));
              if (lookup.size() == 1) {
                shards.insert(std::move(*lookup.begin()));
              }
            } else {
              shards.insert(std::move(val));
            }
          }

          result.MakeInline();
          result.Append(
              state->connection_pool().ExecuteShards(&cloned, info, shards));

        } else {
          // The update statement by itself does not obviously constraint a
          // shard. Try finding the shard(s) via secondary indices.
          ASSIGN_OR_RETURN(
              const auto &pair,
              index::LookupIndex(table_name, info.shard_by,
                                 stmt.GetWhereClause(), state, dataflow_state));
          if (pair.first) {
            // Secondary index available for some constrainted column in stmt.
            result.MakeInline();
            result.AppendDeduplicate(state->connection_pool().ExecuteShards(
                &cloned, info, pair.second));
          } else {
            // Update against all shards.
            result.MakeInline();
            result.Append(state->connection_pool().ExecuteShards(
                &cloned, info, state->UsersOfShard(info.shard_kind)));
          }
        }
      }
    }
  }

  // Delete was successful, time to update dataflows.
  if (update_flows) {
    dataflow_state->ProcessRecords(table_name, records);
  }
  if (!result.IsUpdate()) {
    result = mysql::SqlResult{std::make_unique<mysql::UpdateResult>(0)};
  }

  perf::End("Update");
  return result;
}

}  // namespace update
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
