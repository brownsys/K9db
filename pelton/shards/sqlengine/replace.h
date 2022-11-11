// REPLACE statements sharding and rewriting.
#ifndef PELTON_SHARDS_SQLENGINE_REPLACE_H_
#define PELTON_SHARDS_SQLENGINE_REPLACE_H_

#include <string>

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/dataflow/schema.h"
#include "pelton/shards/types.h"
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
        schema_(conn->state->SharderState().GetTable(table_name_).schema),
        conn_(conn),
        lock_(lock) {}

  /* Main entry point for replace: Executes the statement against the shards. */
  absl::StatusOr<sql::SqlResult> Exec();

 private:
  /* Members. */
  // The REPLACE statement.
  const sqlast::Replace &stmt_;
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

#endif  // PELTON_SHARDS_SQLENGINE_REPLACE_H_
