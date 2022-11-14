// REPLACE statements sharding and rewriting.
#ifndef PELTON_SHARDS_SQLENGINE_REPLACE_H_
#define PELTON_SHARDS_SQLENGINE_REPLACE_H_

#include <string>

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

class ReplaceContext {
 public:
  ReplaceContext(const sqlast::Replace &stmt, Connection *conn,
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

  /* Main entry point for replace: Executes the statement against the shards. */
  absl::StatusOr<sql::SqlResult> Exec();

 private:
  bool CanFastReplace();
  absl::StatusOr<sql::SqlResult> FastReplace();
  absl::StatusOr<sql::SqlResult> DeleteInsert();

  /* Members. */
  // The REPLACE statement.
  const sqlast::Replace &stmt_;
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
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_REPLACE_H_
