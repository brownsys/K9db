// UPDATE statements sharding and rewriting.
#include "pelton/shards/sqlengine/update.h"

#include <memory>

#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/insert.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {

// Update records using the given update statement in memory.
absl::StatusOr<std::vector<sqlast::Insert>> UpdateContext::UpdateRecords(
    const std::vector<dataflow::Record> &records) {
  // What is being updated.
  const std::vector<std::string> &updated_cols = this->stmt_.GetColumns();
  const std::vector<std::string> &updated_vals = this->stmt_.GetValues();
  std::vector<bool> should_update(this->schema_.size(), false);
  for (const std::string &col_name : updated_cols) {
    int col_index = this->schema_.IndexOf(col_name);
    if (col_index < 0) {
      return absl::InvalidArgumentError("Unrecognized column " + col_name);
    }
    should_update.at(col_index) = true;
  }

  // Update one record at a time.
  std::vector<sqlast::Insert> result;
  result.reserve(records.size());
  for (const dataflow::Record &record : records) {
    size_t update_count = 0;
    result.emplace_back(this->table_name_, false);
    for (size_t i = 0; i < this->schema_.size(); i++) {
      if (should_update.at(i)) {
        result.back().AddValue(updated_vals.at(update_count++));
      } else {
        if (record.IsNull(i)) {
          result.back().AddValue("NULL");
          continue;
        }
        switch (this->schema_.TypeOf(i)) {
          case sqlast::ColumnDefinition::Type::UINT:
          case sqlast::ColumnDefinition::Type::INT:
            result.back().AddValue(record.GetValueString(i));
            break;
          case sqlast::ColumnDefinition::Type::TEXT:
          case sqlast::ColumnDefinition::Type::DATETIME:
            result.back().AddValue("\'" + record.GetString(i) + "\'");
            break;
        }
      }
    }
  }

  return result;
}

/*
 * Main entry point for update:
 * Transforms statement into a corresponding delete followed by an insert.
 */
absl::StatusOr<sql::SqlResult> UpdateContext::Exec() {
  // Delete all the records being updated.
  sqlast::Delete delete_stmt = this->stmt_.DeleteDomain();
  DeleteContext del_context(delete_stmt, this->conn_, this->lock_);
  MOVE_OR_RETURN(auto &&[records __COMMA count], del_context.ExecReturning());
  if (count < 0) {
    return sql::SqlResult(count);
  }

  // Perform update over deleted records in memory.
  MOVE_OR_RETURN(std::vector<sqlast::Insert> inserts,
                 this->UpdateRecords(records));

  // Insert updated records!
  for (const sqlast::Insert &insert : inserts) {
    InsertContext context(insert, this->conn_, this->lock_);
    MOVE_OR_RETURN(sql::SqlResult result, context.Exec());
    if (result.UpdateCount() < 0) {
      return sql::SqlResult(result.UpdateCount());
    }
    count += result.UpdateCount();
  }

  // Return number of copies inserted.
  return sql::SqlResult(count);
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
