// REPLACE statements sharding and rewriting.
#include "k9db/shards/sqlengine/replace.h"

#include <memory>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "k9db/dataflow/record.h"
#include "k9db/shards/sqlengine/insert.h"
#include "k9db/shards/sqlengine/update.h"
#include "k9db/util/shard_name.h"
#include "k9db/util/status.h"

namespace k9db {
namespace shards {
namespace sqlengine {

/*
 * Main entry point for replace:
 * Transforms statement into a corresponding delete followed by an insert.
 */
absl::StatusOr<sql::SqlResult> ReplaceContext::Exec() {
  // Make sure table exists in the schema first.
  ASSERT_RET(this->sstate_.TableExists(this->table_name_), InvalidArgument,
             "Table does not exist");

  // Begin the transaction.
  this->db_->BeginTransaction(true);

  // Find out whether the PK exists or not.
  size_t pk = this->schema_.keys().front();
  const std::string &pkcol = this->schema_.NameOf(pk);
  const sqlast::Value &pkval = this->stmt_.GetValue(pkcol, pk);
  if (this->db_->Exists(this->table_name_, pkval)) {
    // Record exists. Update it.
    sqlast::Update update(this->stmt_.table_name());
    for (size_t i = 0; i < this->schema_.size(); i++) {
      if (i == pk) {
        continue;
      }
      const sqlast::Value &v = this->stmt_.GetValue(this->schema_.NameOf(i), i);
      update.AddColumnValue(this->schema_.NameOf(i),
                            std::make_unique<sqlast::LiteralExpression>(v));
    }
    // WHERE pk = <value from replace>.
    sqlast::Expression::Type eq = sqlast::Expression::Type::EQ;
    std::unique_ptr<sqlast::BinaryExpression> where =
        std::make_unique<sqlast::BinaryExpression>(eq);
    where->SetLeft(std::make_unique<sqlast::ColumnExpression>(pkcol));
    where->SetRight(std::make_unique<sqlast::LiteralExpression>(pkval));
    update.SetWhereClause(std::move(where));

    // Execute the update.
    UpdateContext context(update, this->conn_, this->lock_);
    return context.Exec();
  } else {
    // Record does not exist (and we have a lock on it). Insert it.
    InsertContext context(this->stmt_, this->conn_, this->lock_);
    return context.Exec();
  }
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
