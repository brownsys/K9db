// UPDATE statements sharding and rewriting.
#ifndef PELTON_SHARDS_SQLENGINE_UPDATE_H_
#define PELTON_SHARDS_SQLENGINE_UPDATE_H_

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/shards/types.h"
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
        schema_(conn->state->SharderState().GetTable(table_name_).schema),
        conn_(conn),
        lock_(lock) {}

  /* Main entry point for insert: Executes the statement against the shards. */
  absl::StatusOr<sql::SqlResult> Exec();

 private:
  /* Update records using the given update statement in memory. */
  absl::StatusOr<std::vector<sqlast::Insert>> UpdateRecords(
      const std::vector<dataflow::Record> &records);

  /* Members. */
  // Statement being inserted.
  const sqlast::Update &stmt_;
  const std::string &table_name_;
  const dataflow::SchemaRef &schema_;

  // Pelton connection.
  Connection *conn_;

  // Shared Lock so we can read from the states safetly.
  util::SharedLock *lock_;
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_UPDATE_H_
