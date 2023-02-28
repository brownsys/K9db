// GDPR statements handling (FORGET and GET).

#ifndef PELTON_SHARDS_SQLENGINE_GDPR_H_
#define PELTON_SHARDS_SQLENGINE_GDPR_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
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
        db_(conn->session.get()),
        lock_(lock) {}

  absl::StatusOr<sql::SqlResult> Exec();

 private:
  /* Handle forget. */
  absl::StatusOr<sql::SqlResult> ExecForget();

  /* Handle get and its helpers. */
  std::vector<sqlast::Value> ExtractUserIDs(const IndexDescriptor &index,
                                            sqlast::Value &&data_subject);

  std::vector<std::string> AddIntersect(const std::vector<std::string> &into,
                                        const std::vector<std::string> &from);

  bool OwnsRecordThroughDesc(const std::unique_ptr<ShardDescriptor> &owner_desc,
                             const dataflow::Record &record);

  void AddOrAppendAndAnon(const TableName &tbl, sql::SqlResultSet &&set,
                          const std::string &accessed_column = "");

  sqlast::Select MakeAccessSelect(const TableName &tbl,
                                  const std::string &column,
                                  const std::vector<sqlast::Value> &invals);

  sqlast::Update MakeAccessAnonUpdate(
      const TableName &tbl, const std::unordered_set<std::string> &anon_cols,
      const std::string &pk_col, sqlast::Value pk_val);

  void FindData(const TableName &tbl, const ShardDescriptor *desc,
                const std::vector<sqlast::Value> &fkvals, bool forget);

  void FindInDependents(const TableName &table_name,
                        const std::vector<dataflow::Record> &rows, bool forget);

  void AnonAccessors(const TableName &tbl, sql::SqlResultSet &&set,
                     const std::string &accessed_column);

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
  sql::Session *db_;

  // Shared Lock so we can read from the states safetly.
  util::SharedLock *lock_;

  // The result of a GDPR GET operation.
  std::unordered_map<TableName, sql::SqlResultSet> result_;
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_GDPR_H_
