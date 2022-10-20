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
  // Make sure table exists in the schema first.
  ASSERT_RET(this->sstate_.TableExists(this->table_name_), InvalidArgument,
             "Table does not exist");

  // Insert the data into the physical shards.
  sql::SqlResultSet result = this->db_->ExecuteSelect(this->stmt_);

  // Return number of copies inserted.
  return sql::SqlResult(std::move(result));
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
