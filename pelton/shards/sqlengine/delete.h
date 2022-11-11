// DELETE statements sharding and rewriting.
#ifndef PELTON_SHARDS_SQLENGINE_DELETE_H_
#define PELTON_SHARDS_SQLENGINE_DELETE_H_

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
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

  /* Main entry point for delete: Executes the statement against the shards. */
  absl::StatusOr<sql::SqlResult> Exec();
  absl::StatusOr<std::pair<std::vector<dataflow::Record>, int>> ExecReturning();

 private:
  using RecordsIterable = util::Iterable<sql::SqlDeleteSet::ShardRecordsIt>;

  /* Recursively deleting records in dependent tables from the affected
     shard. */
  template <typename Iterable>
  absl::StatusOr<int> DeleteDependents(const Table &table,
                                       const util::ShardName &shard_name,
                                       Iterable &&records);

  /* Members. */
  const sqlast::Delete &stmt_;
  const std::string &table_name_;
  const Table &table_;
  const dataflow::SchemaRef &schema_;

  // Pelton connection.
  Connection *conn_;

  // Connection components.
  SharderState &sstate_;
  dataflow::DataFlowState &dstate_;
  sql::AbstractConnection *db_;

  // Shared Lock so we can read from the states safetly.
  util::SharedLock *lock_;

  // Records that have already been moved to default (to avoid double moving).
  // TableName -> { PKs of records that were moved }
  std::unordered_map<std::string, std::unordered_set<std::string>> moved_;
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_DELETE_H_
