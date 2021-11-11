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

#include "absl/status/status.h"
#include "absl/strings/match.h"

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

    // check indices that start with a ref_
    // 1. extract tablename
    // 2. iterate over index
    // 3. execute queries, SELECT * FROM <table_name> WHERE Accessor_..._id AND shard_by=index.value
    // /or/ to do in one shot: shard_by in (index.value,...)
    // create ast for select{} query, don't hardcode string query
    // restrict to shard
    // user that belongs to the user currently accessing the data

    // if more than one index, then more than one table where user has access but not delete rights
    // extract tablename_colname ==> 
    // identify user we're tying to get info from
    // if doctor, we keep it
    // extract table name out where we find the data (chat)



    // Delete was successful, time to update dataflows.
    if (update_flows) {
      std::vector<dataflow::Record> records =
          table_result.NextResultSet()->Vectorize();
      total_count += records.size();
      dataflow_state->ProcessRecords(table_name, records);
    } else if (is_forget) {
      total_count += table_result.UpdateCount();
    } else if (!is_forget) {
      result.AddResultSet(table_result.NextResultSet());
    }

    // Update result with 
    // for (const auto &column_name : state->IndicesFor(table_name)) {
    //   bool explicit_accessor = absl::StartsWith(column_name, "ACCESSOR_"); 
    //   if (explicit_accessor) {
    //     sqlast::Select tbl_stmt{info.sharded_table_name};
    //     tbl_stmt.AddColumn("*");
    //     table_result.Append(exec.ExecuteShard(&tbl_stmt, shard_kind, user_id,
    //                                           schema, aug_index));
    //   }
    // }
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
