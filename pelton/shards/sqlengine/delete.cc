// DELETE statements sharding and rewriting.
#include "pelton/shards/sqlengine/delete.h"

#include <memory>
#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/util/shard_name.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {

/*
 * Deleting the record from the database.
 */
absl::StatusOr<DeleteContext::DeletedRecords>
DeleteContext::DeleteFromBaseTable() {
  auto &&[set, status] = this->db_->ExecuteDelete(this->stmt_);
  // TODO(babman): need to find shards of records.
  return DeletedRecords{status, set.Vec()};
}

/*
 * Main entry point for insert: Executes the statement against the shards.
 */
absl::StatusOr<sql::SqlResult> DeleteContext::Exec() {
  // Make sure table exists in the schema first.
  ASSERT_RET(this->sstate_.TableExists(this->table_name_), InvalidArgument,
             "Table does not exist");

  // Insert the data into the physical shards.
  MOVE_OR_RETURN(DeletedRecords result, this->DeleteFromBaseTable());
  if (result.status < 0) {
    return sql::SqlResult(result.status);
  }

  // Everything has been inserted; feed to dataflows.
  this->dstate_.ProcessRecords(this->table_name_, std::move(result.records));

  // Return number of copies inserted.
  return sql::SqlResult(result.status);
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
