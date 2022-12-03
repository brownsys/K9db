// EXPLAIN QUERY statements handling.
#ifndef PELTON_SHARDS_SQLENGINE_EXPLAIN_H_
#define PELTON_SHARDS_SQLENGINE_EXPLAIN_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/sql/connection.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/upgradable_lock.h"

namespace pelton {
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

  // Pelton connection.
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
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_EXPLAIN_H_
