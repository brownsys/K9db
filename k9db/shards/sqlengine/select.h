// SELECT statements sharding and rewriting.
#ifndef K9DB_SHARDS_SQLENGINE_SELECT_H_
#define K9DB_SHARDS_SQLENGINE_SELECT_H_

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "absl/status/statusor.h"
#include "k9db/connection.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/state.h"
#include "k9db/shards/state.h"
#include "k9db/shards/types.h"
#include "k9db/sql/connection.h"
#include "k9db/sql/result.h"
#include "k9db/sqlast/ast.h"
#include "k9db/sqlast/value_mapper.h"
#include "k9db/util/upgradable_lock.h"

namespace k9db {
namespace shards {
namespace sqlengine {

class SelectContext {
 public:
  SelectContext(const sqlast::Select &stmt, Connection *conn,
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

  /* Main entry point for select: Executes the statement against the shards. */
  absl::StatusOr<sql::SqlResult> Exec();
  absl::StatusOr<sql::SqlResult> ExecWithinTransaction();

 private:
  /* Members. */
  const sqlast::Select &stmt_;
  const std::string &table_name_;
  const Table &table_;
  const dataflow::SchemaRef &schema_;

  // K9db connection.
  Connection *conn_;

  // Connection components.
  SharderState &sstate_;
  dataflow::DataFlowState &dstate_;
  sql::Session *db_;

  // Shared Lock so we can read from the states safetly.
  util::SharedLock *lock_;

  // Find the relevant <shard, pk> pairs that the query is looking up by
  // analyzing the query, sharding state, and secondary in memory indices.
  // If this returns NONE, then the information was not sufficient to determine
  // this set safetly, and thus we should rely on our rocksdb engine to handle
  // the query.
  std::optional<std::vector<sql::KeyPair>> FindDirectKeys(
      sqlast::ValueMapper *value_mapper);
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db

#endif  // K9DB_SHARDS_SQLENGINE_SELECT_H_
