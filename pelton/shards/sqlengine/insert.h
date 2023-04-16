// INSERT statements sharding and rewriting.
#ifndef PELTON_SHARDS_SQLENGINE_INSERT_H_
#define PELTON_SHARDS_SQLENGINE_INSERT_H_

#include <string>
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

class InsertContext {
 public:
  using Result = std::pair<std::vector<dataflow::Record>, int>;

  InsertContext(const sqlast::Insert &stmt, Connection *conn,
                util::SharedLock *lock)
      : stmt_(stmt),
        table_name_(stmt_.table_name()),
        table_(conn->state->SharderState().GetTable(table_name_)),
        schema_(table_.schema),
        records_(),
        conn_(conn),
        sstate_(conn->state->SharderState()),
        dstate_(conn->state->DataflowState()),
        db_(conn->session.get()),
        lock_(lock),
        shards_(),
        new_users_(0) {}

  /* Main entry point for insert: Executes the statement against the shards. */
  absl::StatusOr<sql::SqlResult> Exec();

 private:
  /* Helpers for inserting statement into the database by sharding type. */
  absl::Status DirectInsert(sqlast::Value &&fkval, const ShardDescriptor &desc);
  absl::Status TransitiveInsert(sqlast::Value &&fkval,
                                const ShardDescriptor &desc);
  absl::Status VariableInsert(sqlast::Value &&fkval,
                              const ShardDescriptor &desc);

  /* Inserting the statement into the database. */
  absl::StatusOr<int> InsertIntoBaseTable();

  /* Recursively moving/copying records in dependent tables into the affected
     shard. */
  absl::StatusOr<int> CascadeDependents();

  /* Members. */
  // Statement being inserted.
  const sqlast::Insert &stmt_;
  const std::string &table_name_;
  const Table &table_;
  const dataflow::SchemaRef &schema_;
  std::vector<dataflow::Record> records_;

  // Pelton connection.
  Connection *conn_;

  // Connection components.
  SharderState &sstate_;
  dataflow::DataFlowState &dstate_;
  sql::Session *db_;

  // Shared Lock so we can read from the states safetly.
  util::SharedLock *lock_;

  // Store all the shards we should insert into.
  std::unordered_set<util::ShardName> shards_;

  // How many new users have we inserted.
  size_t new_users_ = 0;
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_INSERT_H_
