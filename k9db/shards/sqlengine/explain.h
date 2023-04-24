// EXPLAIN QUERY statements handling.
#ifndef K9DB_SHARDS_SQLENGINE_EXPLAIN_H_
#define K9DB_SHARDS_SQLENGINE_EXPLAIN_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "k9db/connection.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/state.h"
#include "k9db/shards/state.h"
#include "k9db/sql/connection.h"
#include "k9db/sql/result.h"
#include "k9db/sqlast/ast.h"
#include "k9db/util/upgradable_lock.h"

namespace k9db {
namespace shards {
namespace sqlengine {

class ExplainContext {
 public:
  ExplainContext(const sqlast::ExplainQuery &stmt, Connection *conn,
                 util::SharedLock *lock)
      : stmt_(stmt),
        conn_(conn),
        sstate_(conn->state->SharderState()),
        dstate_(conn->state->DataflowState()),
        db_(conn->state->Database()),
        lock_(lock),
        schema_(dataflow::SchemaFactory::EXPLAIN_QUERY_SCHEMA) {}

  absl::StatusOr<sql::SqlResult> Exec();

 private:
  /* Explain by type of query. */
  void Explain(const sqlast::Insert &query);
  void Explain(const sqlast::Replace &query);
  void Explain(const sqlast::Update &query);
  void Explain(const sqlast::Delete &query);
  void Explain(const sqlast::Select &query);

  /* Recurse on dependent tables. */
  void RecurseInsert(const std::string &table_name, bool first);
  void RecurseDelete(const std::string &shard_kind,
                     const std::string &table_name, bool first);

  /* Members. */
  const sqlast::ExplainQuery &stmt_;

  // K9db connection.
  Connection *conn_;

  // Connection components.
  SharderState &sstate_;
  dataflow::DataFlowState &dstate_;
  sql::Connection *db_;

  // Shared Lock so we can read from the states safetly.
  util::SharedLock *lock_;

  // The result gets stored here.
  using Explanation = std::vector<std::pair<std::string, std::string>>;
  dataflow::SchemaRef schema_;
  std::vector<dataflow::Record> explanation_;

  void AddExplanation(const std::string &type, const std::string &details) {
    this->explanation_.emplace_back(this->schema_, true,
                                    std::make_unique<std::string>(type),
                                    std::make_unique<std::string>(details));
  }
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db

#endif  // K9DB_SHARDS_SQLENGINE_EXPLAIN_H_
