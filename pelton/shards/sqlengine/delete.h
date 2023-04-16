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
#include "pelton/sql/connection.h"
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
        db_(conn->session.get()),
        lock_(lock) {}

  /* Main entry point for delete: Executes the statement against the shards. */
  absl::StatusOr<sql::SqlResult> Exec();

  using Result = std::pair<int, std::vector<dataflow::Record>>;
  absl::StatusOr<Result> ExecWithinTransaction();
  absl::StatusOr<Result> DeleteAnonymize();

 private:
  /* Check FK integrity */
  absl::Status CheckFKIntegrity(const sql::SqlDeleteSet &delset);

  /* Recursively moving/copying records in dependent tables into the affected
     shard. */
  absl::StatusOr<int> CascadeDependents(const sql::SqlDeleteSet &delset);

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
  sql::Session *db_;

  // Shared Lock so we can read from the states safetly.
  util::SharedLock *lock_;
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_DELETE_H_
