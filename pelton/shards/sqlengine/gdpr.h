// GDPR statements handling (FORGET and GET).

#ifndef PELTON_SHARDS_SQLENGINE_GDPR_H_
#define PELTON_SHARDS_SQLENGINE_GDPR_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace sqlengine {

class GDPRContext {
 public:
  GDPRContext(const sqlast::GDPRStatement &stmt, Connection *conn,
              util::SharedLock *lock)
      : stmt_(stmt),
        shard_kind_(stmt.shard_kind()),
        user_id_(stmt.user_id()),
        user_id_str_(user_id_.AsUnquotedString()),
        conn_(conn),
        sstate_(conn->state->SharderState()),
        dstate_(conn->state->DataflowState()),
        db_(conn->state->Database()),
        lock_(lock) {}

  absl::StatusOr<sql::SqlResult> Exec();

 private:
  /* Handle forget. */
  absl::StatusOr<sql::SqlResult> ExecForget();

  /* Handle get and its helpers. */
  void AddOrAppend(const TableName &tbl, sql::SqlResultSet &&set);

  sqlast::Select MakeAccessSelect(const TableName &tbl,
                                  const std::string &column,
                                  const std::vector<sqlast::Value> &invals);

  void FindData(const TableName &tbl, const ShardDescriptor *desc,
                const std::vector<sqlast::Value> &fkvals);

  void FindInDependents(const TableName &table_name,
                        const std::vector<dataflow::Record> &rows);

  absl::StatusOr<sql::SqlResult> ExecGet();

  /* Members. */
  const sqlast::GDPRStatement &stmt_;
  const std::string &shard_kind_;
  const sqlast::Value &user_id_;
  std::string user_id_str_;

  // Pelton connection.
  Connection *conn_;

  // Connection components.
  SharderState &sstate_;
  dataflow::DataFlowState &dstate_;
  sql::AbstractConnection *db_;

  // Shared Lock so we can read from the states safetly.
  util::SharedLock *lock_;

  // The result of a GDPR GET operation.
  std::unordered_map<TableName, sql::SqlResultSet> result_;
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_GDPR_H_
