// INSERT statements sharding and rewriting.
#ifndef PELTON_SHARDS_SQLENGINE_INSERT_H_
#define PELTON_SHARDS_SQLENGINE_INSERT_H_

#include <string>
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

class InsertContext {
 public:
  InsertContext(const sqlast::Insert &stmt, Connection *conn,
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
  /* Helpers for inserting statement into the database by sharding type. */
  int DirectInsert(dataflow::Value &&fkval);
  absl::StatusOr<int> TransitiveInsert(dataflow::Value &&fkval,
                                       const ShardDescriptor &desc);
  absl::StatusOr<int> VariableInsert(dataflow::Value &&fkval,
                                     const ShardDescriptor &desc);

  /* Inserting the statement into the database. */
  absl::StatusOr<int> InsertIntoBaseTable();

  /* Recursively moving/copying records in dependent tables into the affected
     shard. */

  /* Members. */
  // Statement being inserted.
  const sqlast::Insert &stmt_;
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

  // This vector will store the shardname for each insert.
  std::vector<std::unordered_set<std::string>> shard_names_;
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_INSERT_H_
