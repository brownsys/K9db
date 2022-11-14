// REPLACE statements sharding and rewriting.
#include "pelton/shards/sqlengine/replace.h"

#include <memory>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"
#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/insert.h"
#include "pelton/util/shard_name.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {

bool ReplaceContext::CanFastReplace() {
  if (this->table_.owners.size() >= 2 || this->table_.dependents.size() > 0) {
    return false;
  }
  if (this->table_.owners.size() == 1) {
    return this->table_.owners.front()->type == InfoType::DIRECT;
  }
  return true;
}

absl::StatusOr<sql::SqlResult> ReplaceContext::FastReplace() {
  // Need to insert a copy for each way of sharding the table.
  CHECK_LE(this->table_.owners.size(), 1u);
  util::ShardName shard_name(DEFAULT_SHARD, DEFAULT_SHARD);

  // Find owner shard if table is owned.
  if (this->table_.owners.size() == 1) {
    const std::unique_ptr<ShardDescriptor> &desc = this->table_.owners.front();

    // Identify value of the column along which we are sharding.
    size_t colidx = EXTRACT_VARIANT(column_index, desc->info);
    const std::string &colname = EXTRACT_VARIANT(column, desc->info);
    sqlast::Value val = this->stmt_.GetValue(colname, colidx);
    ASSERT_RET(!val.IsNull(), Internal, "OWNER cannot be NULL");

    // Must be direct sharding for fast replace.
    CHECK(desc->type == InfoType::DIRECT);
    std::string user_id = val.AsUnquotedString();
    shard_name = util::ShardName(desc->shard_kind, user_id);
  }

  // Perform fast replace.
  auto pair = this->db_->ExecuteReplace(this->stmt_, shard_name);
  if (pair.second < 0) {
    return sql::SqlResult(pair.second);
  }

  // update dataflow.
  std::vector<dataflow::Record> records;
  if (pair.first.has_value()) {
    records.push_back(std::move(pair.first.value()));
  }
  records.push_back(this->dstate_.CreateRecord(this->stmt_));
  this->dstate_.ProcessRecords(this->table_name_, std::move(records));

  // Done.
  return sql::SqlResult(pair.second);
}

absl::StatusOr<sql::SqlResult> ReplaceContext::DeleteInsert() {
  LOG(WARNING) << "SLOW REPLACE " << this->stmt_;
  // Find PK.
  CHECK_EQ(this->schema_.keys().size(), 1u) << "Replace table with illegal PK";
  size_t pk_index = this->schema_.keys().front();
  const std::string &pk_column = this->schema_.NameOf(pk_index);
  const sqlast::Value &pk_value = this->stmt_.GetValue(pk_column, pk_index);

  // Delete the record being replaced (if exists).
  sqlast::Delete delete_stmt(this->table_name_);
  using EType = sqlast::Expression::Type;
  auto where = std::make_unique<sqlast::BinaryExpression>(EType::EQ);
  where->SetLeft(std::make_unique<sqlast::ColumnExpression>(pk_column));
  where->SetRight(std::make_unique<sqlast::LiteralExpression>(pk_value));
  delete_stmt.SetWhereClause(std::move(where));

  DeleteContext del_context(delete_stmt, this->conn_, this->lock_);
  MOVE_OR_RETURN(sql::SqlResult result, del_context.Exec());
  if (result.UpdateCount() < 0) {
    return sql::SqlResult(result.UpdateCount());
  }

  // Insert the record.
  auto casted = static_cast<const sqlast::Insert *>(&this->stmt_);
  InsertContext context(*casted, this->conn_, this->lock_);
  MOVE_OR_RETURN(sql::SqlResult result2, context.Exec());
  if (result2.UpdateCount() < 0) {
    return sql::SqlResult(result2.UpdateCount());
  }

  // Return the total number of operations performed against the DB.
  return sql::SqlResult(result.UpdateCount() + result2.UpdateCount());
}

/*
 * Main entry point for replace:
 * Transforms statement into a corresponding delete followed by an insert.
 */
absl::StatusOr<sql::SqlResult> ReplaceContext::Exec() {
  if (this->CanFastReplace()) {
    return this->FastReplace();
  } else {
    return this->DeleteInsert();
  }
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
