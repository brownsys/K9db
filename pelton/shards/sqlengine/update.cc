// UPDATE statements sharding and rewriting.
#include "pelton/shards/sqlengine/update.h"

#include <string>
#include <vector>

#include "pelton/shards/sqlengine/select.h"
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

absl::Status UpdateRawRecords(std::vector<RawRecord> *records,
                              const sqlast::Update &stmt,
                              const sqlast::CreateTable &table_schema) {
  // What is being updated.
  const std::string &table_name = stmt.table_name();
  const std::vector<std::string> &updated_cols = stmt.GetColumns();
  const std::vector<std::string> &updated_vals = stmt.GetValues();

  // Loop over records and apply update.
  size_t size = records->size();
  for (size_t i = 0; i < size; i++) {
    const RawRecord &record = records->at(i);
    std::vector<std::string> updated_record = record.values;
    for (size_t c = 0; c < updated_cols.size(); c++) {
      const std::string &col_name = updated_cols.at(c);
      int val_index = IndexOfColumn(col_name, table_schema);
      if (val_index < 0) {
        return absl::InvalidArgumentError("Unrecognized column " + col_name);
      }
      updated_record.at(val_index) = updated_vals.at(c);
    }
    records->emplace_back(table_name, updated_record, record.columns, true);
  }

  return absl::OkStatus();
}

}  // namespace

absl::Status Shard(const sqlast::Update &stmt, SharderState *state,
                   dataflow::DataFlowState *dataflow_state,
                   const OutputChannel &output) {
  // Table name to select from.
  const std::string &table_name = stmt.table_name();

  // Get the rows that are going to be deleted prior to deletion to use them
  // to update the dataflows.
  std::vector<RawRecord> records;
  CHECK_STATUS(select::Query(&records, stmt.SelectDomain(), state,
                             dataflow_state, false));
  CHECK_STATUS(UpdateRawRecords(&records, stmt, state->GetSchema(table_name)));

  sqlast::Stringifier stringifier;
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    // Case 1: table is not in any shard.
    std::string update_str = stmt.Visit(&stringifier);
    CHECK_STATUS(state->connection_pool().ExecuteDefault(update_str, output));

  } else {  // is_sharded == true
    // Case 2: table is sharded.
    // The table might be sharded according to different column/owners.
    // We must update all these different duplicates.
    for (const auto &info : state->GetShardingInformation(table_name)) {
      if (stmt.AssignsTo(info.shard_by)) {
        return absl::InvalidArgumentError("Update cannot modify owner column");
      }

      // Rename the table to match the sharded name.
      sqlast::Update cloned = stmt;
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
        std::string update_str = cloned.Visit(&stringifier);
        CHECK_STATUS(state->connection_pool().ExecuteShard(update_str, info,
                                                           user_id, output));
      } else {
        // Update against the relevant shards.
        std::string update_str = cloned.Visit(&stringifier);
        CHECK_STATUS(state->connection_pool().ExecuteShard(
            update_str, info, state->UsersOfShard(info.shard_kind), output));
      }
    }
  }

  // Delete was successful, time to update dataflows.
  dataflow_state->ProcessRawRecords(records);

  return absl::OkStatus();
}

}  // namespace update
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
