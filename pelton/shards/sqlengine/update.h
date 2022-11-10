// UPDATE statements sharding and rewriting.
#ifndef PELTON_SHARDS_SQLENGINE_UPDATE_H_
#define PELTON_SHARDS_SQLENGINE_UPDATE_H_

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/dataflow/record.h"
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

class UpdateContext {
 public:
  UpdateContext(const sqlast::Update &stmt, Connection *conn,
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

  /* Main entry point for update: Executes the statement against the shards. */
  absl::StatusOr<sql::SqlResult> Exec();

 private:
  /* Returns true if the update statement may change the sharding/ownership of
     any affected record in the table or dependent tables. False guarantees
     the update is a no-op wrt ownership. */
  bool ModifiesSharding() const;

  /* Executes the update by issuing a delete followed by an insert. */
  absl::StatusOr<sql::SqlResult> DeleteInsert();

  /* Executes the update directly against the database by overriding data
     in the database. */
  absl::StatusOr<sql::SqlResult> DirectUpdate();

  /* Update records using the given update statement in memory. */
  absl::StatusOr<std::vector<sqlast::Insert>> UpdateRecords(
      const std::vector<dataflow::Record> &records);

  /* Members. */
  const sqlast::Update &stmt_;
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

#endif  // PELTON_SHARDS_SQLENGINE_UPDATE_H_
