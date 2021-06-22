// GDPR statements handling (FORGET and GET).

#include "pelton/shards/sqlengine/gdpr.h"

#include <utility>

#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace gdpr {

absl::StatusOr<mysql::SqlResult> Shard(
    const sqlast::GDPRStatement &stmt, SharderState *state,
    dataflow::DataFlowState *dataflow_state) {
  perf::Start("GDPR");
  mysql::SqlResult result;

  uint64_t total_count = 0;
  const std::string &shard_kind = stmt.shard_kind();
  const std::string &user_id = stmt.user_id();
  bool is_forget = stmt.operation() == sqlast::GDPRStatement::Operation::FORGET;
  for (const auto &table_name : state->TablesInShard(shard_kind)) {
    result.MakeInline();

    mysql::SqlResult table_result;
    dataflow::SchemaRef schema = dataflow_state->GetTableSchema(table_name);
    bool update_flows = is_forget && dataflow_state->HasFlowsFor(table_name);
    for (const auto &info : state->GetShardingInformation(table_name)) {
      table_result.MakeInline();
      if (info.shard_kind != shard_kind) {
        continue;
      }

      if (is_forget) {
        sqlast::Delete tbl_stmt{info.sharded_table_name, update_flows};
        table_result.Append(state->connection_pool().ExecuteShard(
            &tbl_stmt, info, user_id, schema));
      } else {  // sqlast::GDPRStatement::Operation::GET
        sqlast::Select tbl_stmt{info.sharded_table_name};
        tbl_stmt.AddColumn("*");
        table_result.Append(state->connection_pool().ExecuteShard(
            &tbl_stmt, info, user_id, schema));
      }
    }

    // Delete was successful, time to update dataflows.
    if (update_flows) {
      std::vector<dataflow::Record> records = table_result.Vectorize();
      total_count += records.size();
      dataflow_state->ProcessRecords(table_name, records);
    } else if (is_forget) {
      total_count += table_result.UpdateCount();
    } else if (!is_forget) {
      // TODO(babman): results here should be returned instead of printed,
      //               we need to be able to have a mysql::SqlResult with multi
      //               result sets over multiple tables.
      // result.Append(std::move(table_result));
      std::cout << table_name << std::endl;
      std::cout << schema << std::endl;
      for (const dataflow::Record &record : table_result.Vectorize()) {
        std::cout << record << std::endl;
      }
    }
  }

  perf::End("GDPR");
  if (is_forget) {
    return mysql::SqlResult{std::make_unique<mysql::UpdateResult>(total_count)};
  }
  return result;
}

}  // namespace gdpr
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
