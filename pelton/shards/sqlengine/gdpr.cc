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

absl::StatusOr<sql::SqlResult> Shard(const sqlast::GDPRStatement &stmt,
                                     SharderState *state,
                                     dataflow::DataFlowState *dataflow_state) {
  perf::Start("GDPR");
  sql::SqlResult result;

  int total_count = 0;
  const std::string &shard_kind = stmt.shard_kind();
  const std::string &user_id = stmt.user_id();
  bool is_forget = stmt.operation() == sqlast::GDPRStatement::Operation::FORGET;
  auto &exec = state->executor();
  for (const auto &table_name : state->TablesInShard(shard_kind)) {
    sql::SqlResult table_result;
    dataflow::SchemaRef schema = dataflow_state->GetTableSchema(table_name);
    bool update_flows = is_forget && dataflow_state->HasFlowsFor(table_name);
    for (const auto &info : state->GetShardingInformation(table_name)) {
      if (info.shard_kind != shard_kind) {
        continue;
      }

      // Augment returned results with the user_id.
      int aug_index = -1;
      if (!info.IsTransitive()) {
        aug_index = info.shard_by_index;
      }

      if (is_forget) {
        sqlast::Delete tbl_stmt{info.sharded_table_name, update_flows};
        table_result.Append(exec.ExecuteShard(&tbl_stmt, shard_kind, user_id,
                                              schema, aug_index));
      } else {  // sqlast::GDPRStatement::Operation::GET
        sqlast::Select tbl_stmt{info.sharded_table_name};
        tbl_stmt.AddColumn("*");
        table_result.Append(exec.ExecuteShard(&tbl_stmt, shard_kind, user_id,
                                              schema, aug_index));
      }
    }

    // Delete was successful, time to update dataflows.
    if (update_flows) {
      std::vector<dataflow::Record> records =
          table_result.NextResultSet()->Vectorize();
      total_count += records.size();
      dataflow_state->ProcessRecords(table_name, std::move(records));
    } else if (is_forget) {
      total_count += table_result.UpdateCount();
    } else if (!is_forget) {
      result.AddResultSet(table_result.NextResultSet());
    }
  }

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
