// GDPR statements handling (FORGET and GET).
#include "pelton/shards/sqlengine/gdpr.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace gdpr {

absl::StatusOr<sql::SqlResult> Shard(
    const sqlast::GDPRStatement &stmt, SharderState *state,
    dataflow::DataFlowEngine *dataflow_engine) {
  perf::Start("GDPR");
  sql::SqlResult result;

  int total_count = 0;
  const std::string &shard_kind = stmt.shard_kind();
  const std::string &user_id = stmt.user_id();
  bool is_forget = stmt.operation() == sqlast::GDPRStatement::Operation::FORGET;
  for (const auto &table_name : state->TablesInShard(shard_kind)) {
    sql::SqlResult table_result;
    dataflow::SchemaRef schema = dataflow_engine->GetTableSchema(table_name);
    bool update_flows = is_forget && dataflow_engine->HasFlowsFor(table_name);
    for (const auto &info : state->GetShardingInformation(table_name)) {
      if (info.shard_kind != shard_kind) {
        continue;
      }

      if (is_forget) {
        sqlast::Delete tbl_stmt{info.sharded_table_name, update_flows};
        table_result.Append(
            state->executor().ExecuteShard(&tbl_stmt, info, user_id, schema));
      } else {  // sqlast::GDPRStatement::Operation::GET
        sqlast::Select tbl_stmt{info.sharded_table_name};
        tbl_stmt.AddColumn("*");
        table_result.Append(
            state->executor().ExecuteShard(&tbl_stmt, info, user_id, schema));
      }
    }

    // Delete was successful, time to update dataflows.
    if (update_flows) {
      std::vector<dataflow::Record> records =
          table_result.NextResultSet()->Vectorize();
      total_count += records.size();
      dataflow_engine->ProcessRecords(table_name, std::move(records));
    } else if (is_forget) {
      total_count += table_result.UpdateCount();
    } else if (!is_forget) {
      result.AddResultSet(table_result.NextResultSet());
    }
  }
  std::cout << stmt.shard_kind() << " :: " << stmt.user_id() << std::endl;

  perf::End("GDPR");
  if (is_forget) {
    return sql::SqlResult(total_count);
  }
  return result;
}

}  // namespace gdpr
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
