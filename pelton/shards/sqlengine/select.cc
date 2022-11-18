// SELECT statements sharding and rewriting.
#include "pelton/shards/sqlengine/select.h"

#include <utility>

#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {

/*
 * Main entry point for select: Executes the statement against the shards.
 */
absl::StatusOr<sql::SqlResult> SelectContext::Exec() {
  // Start transaction.
  this->db_->BeginTransaction();

  auto result = this->ExecWithinTransaction();

  // Nothing to commit; read only.
  this->db_->RollbackTransaction();

  return result;
}

absl::StatusOr<sql::SqlResult> SelectContext::ExecWithinTransaction() {
  // Make sure table exists in the schema first.
  ASSERT_RET(this->sstate_.TableExists(this->table_name_), InvalidArgument,
             "Table does not exist");
  return sql::SqlResult(this->db_->ExecuteSelect(this->stmt_));
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
