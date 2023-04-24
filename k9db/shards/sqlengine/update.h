// UPDATE statements sharding and rewriting.
#ifndef K9DB_SHARDS_SQLENGINE_UPDATE_H_
#define K9DB_SHARDS_SQLENGINE_UPDATE_H_

#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "k9db/connection.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/state.h"
#include "k9db/shards/state.h"
#include "k9db/shards/types.h"
#include "k9db/sql/connection.h"
#include "k9db/sql/result.h"
#include "k9db/sqlast/ast.h"
#include "k9db/util/shard_name.h"
#include "k9db/util/upgradable_lock.h"

namespace k9db {
namespace shards {
namespace sqlengine {

struct UpdateInfo {
  dataflow::Record old;
  dataflow::Record updated;
  std::unordered_set<util::ShardName> old_shards;
  std::unordered_set<util::ShardName> new_shards;
};

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
        db_(conn->session.get()),
        lock_(lock),
        update_columns_() {}

  /* Main entry point for update: Executes the statement against the shards. */
  absl::StatusOr<sql::SqlResult> Exec();

  using Result = std::pair<int, std::vector<dataflow::Record>>;
  absl::StatusOr<Result> ExecWithinTransaction();
  absl::StatusOr<Result> UpdateAnonymize();

 private:
  /* Returns true if the update statement may change the sharding/ownership of
     any affected record in the table.
     False guarantees the update is a no-op wrt to who owns any affected
     records. */
  absl::StatusOr<bool> ModifiesShardingBaseTable();

  /* Same but for dependent tables. */
  bool ModifiesShardingDependentTables();

  /* Executes the update by issuing a delete followed by an insert. */
  absl::StatusOr<std::vector<UpdateInfo>> DeleteInsert();

  /* Executes the update directly against the database by overriding data
     in the database. */
  std::vector<UpdateInfo> DirectUpdate();

  /* Update records using the given update statement in memory. */
  absl::StatusOr<std::vector<sqlast::Insert>> UpdateRecords(
      std::vector<UpdateInfo> *infos) const;

  /* Locate the shards that this new record should be inserted to by looking at
     parent tables. */
  absl::StatusOr<std::unordered_set<util::ShardName>> LocateNewShards(
      const dataflow::Record &record) const;

  /* Cascade shard changes to dependent tables. */
  absl::Status CascadeDependents(const std::vector<UpdateInfo> &cascades);

  /* Members. */
  const sqlast::Update &stmt_;
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

  // The columns that this statement update.
  std::unordered_set<size_t> update_columns_;
  std::vector<dataflow::Record> negatives_;
  std::vector<dataflow::Record> positives_;
  int status_ = 0;
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db

#endif  // K9DB_SHARDS_SQLENGINE_UPDATE_H_
