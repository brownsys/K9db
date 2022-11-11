// REPLACE statements sharding and rewriting.
#include "pelton/shards/sqlengine/replace.h"

#include <memory>
#include <utility>

#include "glog/logging.h"
#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/insert.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {

/*
 * Main entry point for replace:
 * Transforms statement into a corresponding delete followed by an insert.
 */
absl::StatusOr<sql::SqlResult> ReplaceContext::Exec() {
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

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
