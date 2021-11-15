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

    // get data for an accessor
    // for every table in this shard
    for (const auto &table_name : state->TablesInShard(shard_kind)) {
      // for every index associated with this table
      for (const auto &index : state->IndicesFor(table_name) {
        // check if index starts with ref_ + shard_kind, indicating an accessor
        bool explicit_accessor = absl::StartsWith(index, "ref_" + shard_kind); 
        if (explicit_accessor){
          // 1. extract tablename from index
          // remove basename_typeofuser from index_name <basename_typeofuser/shardkind_tablename_accessorcol>
          // e.g. ref_doctors from ref_doctors_chat_colname
          // know that colname has to start with "_ACCESSOR" so find substring before it
          std::String accessor_table_name = "";
          // TODO: index.substr(1, index.find("ACCESSOR_"));

          // 2. extract shardbycol
          // TODO: get accessor colname
          std::String accessor_col_name = "";

          // 3. query this table (iterate over all values in the index that correspond to the user)
          // requesting the data with user_id

          // SELECT * FROM <table_name> WHERE Accessor_doctor_id = user_id AND OWNER_patient_id in (index.value, ...)
          sqlast::Select accessor_stmt{accessor_table_name};
          // TODO: add where condition via ast_expressions.h, constructing children first, then adding parents
          // 1. Accessor_doctor_id = user_id --> left condition
          // ===> binary expression. type=equality
          // ====> left of left: columnExpression (colname). right of right: literalExpression (value/user_id)
          sqlast::BinaryExpression{...}
          // 2. OWNER_patient_id in (index.value, ...) --> right condition
          // ===> binary expression. type=in
          // ====> left of left: columnExpression (colname). right of right: LiteralListExpression
          // index.h, 2nd method called LookupIndex(index, user_id, dataflow_state), returns set of user_ids to pass to LiteralListExpression(vector<std::string>)
          // ==> each id is that of patient related to the doctor (that have exchanged messages)
          // shard_by is owner_patient_id, get from state->shardingInfo
          accessor_stmt.AddColumn("*");

          absl::StatusOr<sql::SqlResult> accessor_data = select::Shard(accessor_stmt, state, dataflow_state);
          table_result.Append(accessor_data);
        }
      }
    }

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
