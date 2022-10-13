// DELETE statements sharding and rewriting.
#ifndef PELTON_SHARDS_SQLENGINE_DELETE_H_
#define PELTON_SHARDS_SQLENGINE_DELETE_H_

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
#include "pelton/dataflow/value.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sql/abstract_connection.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/upgradable_lock.h"

namespace pelton {
namespace shards {
namespace sqlengine {

class DeleteContext {
 public:
  DeleteContext(const sqlast::Delete &stmt, Connection *conn,
                util::SharedLock *lock)
      : stmt_(stmt),
        table_name_(stmt_.table_name()),
        table_(conn->state->SharderState().GetTable(table_name_)),
        schema_(table_.schema),
        conn_(conn),
        sstate_(conn->state->SharderState()),
        dstate_(conn->state->DataflowState()),
        db_(conn->state->Database()),
        lock_(lock) {}

  /* Main entry point for insert: Executes the statement against the shards. */
  absl::StatusOr<sql::SqlResult> Exec();

 private:
  // The records we delete plus their sharding association.
  struct DeletedRecords {
    int status;
    std::vector<dataflow::Record> records;
    // pointers into records.
    std::unordered_map<util::ShardName, std::vector<dataflow::Record *>> shards;
  };

  /* Deleting the record from the database. */
  absl::StatusOr<DeletedRecords> DeleteFromBaseTable();

  /* Recursively deleting records in dependent tables from the affected
     shard.
  absl::StatusOr<int> DeleteDependentsFromShard(
      const Table &table, const std::vector<dataflow::Record> &records);
  */

  /* Members. */
  const sqlast::Delete &stmt_;
  const std::string &table_name_;
  Table &table_;
  const dataflow::SchemaRef &schema_;

  // Pelton connection.
  Connection *conn_;

  // Connection components.
  SharderState &sstate_;
  dataflow::DataFlowState &dstate_;
  sql::AbstractConnection *db_;

  // Shared Lock so we can read from the states safetly.
  util::SharedLock *lock_;
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_DELETE_H_
