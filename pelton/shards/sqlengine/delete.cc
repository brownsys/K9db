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
absl::StatusOr<sql::SqlDeleteSet> DeleteContext::DeleteFromBaseTable() {
  return this->db_->ExecuteDelete(this->stmt_);
}

/*
 * Main entry point for delete: Executes the statement against the shards.
 */
absl::StatusOr<sql::SqlResult> DeleteContext::Exec() {
  // Make sure table exists in the schema first.
  ASSERT_RET(this->sstate_.TableExists(this->table_name_), InvalidArgument,
             "Table does not exist");

  // Insert the data into the physical shards.
  MOVE_OR_RETURN(sql::SqlDeleteSet result, this->DeleteFromBaseTable());

  size_t total_count = 0;
  for (const util::ShardName &shard_name : result.IterateShards()) {
    for (const dataflow::Record &record : result.Iterate(shard_name)) {
      total_count++;
    }
  }

  // Everything has been inserted; feed to dataflows.
  this->dstate_.ProcessRecords(this->table_name_, result.Vec());

  // Return number of copies inserted.
  return sql::SqlResult(total_count);
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
