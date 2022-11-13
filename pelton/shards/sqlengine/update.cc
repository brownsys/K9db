// UPDATE statements sharding and rewriting.
#include "pelton/shards/sqlengine/update.h"

#include <memory>
#include <unordered_set>
#include <utility>

#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/insert.h"
#include "pelton/util/iterator.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {

// Update records using the given update statement in memory.
absl::StatusOr<std::vector<sqlast::Insert>> UpdateContext::UpdateRecords(
    const std::vector<dataflow::Record> &records) {
  // Turn SQL statement to a map of updates.
  dataflow::Record::UpdateMap updates;
  const std::vector<std::string> &cols = this->stmt_.GetColumns();
  const auto &vals = this->stmt_.GetValues();
  for (const auto &[col, v] : util::Zip(&cols, &vals)) {
    updates.emplace(col, v.get());
  }

  // Update one record at a time.
  std::vector<sqlast::Insert> result;
  result.reserve(records.size());
  for (const dataflow::Record &record : records) {
    dataflow::Record updated = record.Update(updates);
    std::vector<sqlast::Value> values;
    values.reserve(this->schema_.size());
    for (size_t i = 0; i < this->schema_.size(); i++) {
      values.push_back(updated.GetValue(i));
    }
    result.emplace_back(this->table_name_);
    result.back().SetValues(std::move(values));
  }

  return result;
}

/* Returns true if the update statement may change the sharding/ownership of
     any affected record in the table or dependent tables. False guarantees
     the update is a no-op wrt ownership. */
bool UpdateContext::ModifiesSharding() const {
  // Build set of sharding-relevant columns.
  std::unordered_set<std::string> columns;
  for (size_t i : this->schema_.keys()) {  // PK columns
    columns.insert(this->schema_.NameOf(i));
  }
  for (const std::unique_ptr<ShardDescriptor> &desc : this->table_.owners) {
    columns.insert(EXTRACT_VARIANT(column, desc->info));  // Shard by columns.
  }

  // Find whether any of the columns being updated are sharding relevant!
  for (const std::string &column : this->stmt_.GetColumns()) {
    if (columns.find(column) != columns.end()) {
      return true;
    }
  }
  return false;
}

/* Executes the update by issuing a delete followed by an insert. */
absl::StatusOr<sql::SqlResult> UpdateContext::DeleteInsert() {
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

/* Executes the update directly against the database by overriding data
   in the database. */
absl::StatusOr<sql::SqlResult> UpdateContext::DirectUpdate() {
  sql::ResultSetAndStatus result = this->db_->ExecuteUpdate(this->stmt_);
  if (result.second >= 0) {
    this->dstate_.ProcessRecords(this->table_name_, result.first.Vec());
  }
  return sql::SqlResult(result.second);
}

/*
 * Main entry point for update:
 * Transforms statement into a corresponding delete followed by an insert.
 */
absl::StatusOr<sql::SqlResult> UpdateContext::Exec() {
  if (this->ModifiesSharding()) {
    return this->DeleteInsert();
  } else {
    return this->DirectUpdate();
  }
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
