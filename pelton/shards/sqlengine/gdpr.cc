// GDPR statements handling (FORGET and GET).
#include "pelton/shards/sqlengine/gdpr.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/index.h"
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

  // TODO: fix loop to get indices of chat (chat not stored in doctor shard,
  // only in patient shard hence won't have access to the chat's indices) get
  // data for an accessor for every table in this shard
  for (auto &[shard_kind_access, _] : state->GetKinds()) {
    std::cout << "SHARD_KIND_ACCESS: " << shard_kind_access << std::endl;
    for (const auto &table_name_access :
         state->TablesInShard(shard_kind_access)) {
      for (const auto &index_col : state->IndicesFor(table_name_access)) {
        std::list<ShardingInformation> infos =
            state->GetShardingInformation(table_name_access);
        std::string shard_by_access = "";
        for (const auto &info : infos) {
          shard_by_access = info.shard_by;
        }
        std::cout << "TABLE_NAME_ACCESS: " << table_name_access << std::endl;
        std::cout << "INDEX_COL: " << index_col << std::endl;
        std::cout << "SHARD_BY_ACCESS: " << shard_by_access << std::endl;
        std::string index_name =
            state->IndexFlow(table_name_access, index_col, shard_by_access);

        std::cout << "INDEX_NAME: " << index_name << std::endl;
        // check if index starts with ref_ + shard_kind, indicating an accessor
        // if this index belongs to the (doctor) shard we're GDPR GETing for
        bool explicit_accessor =
            absl::StartsWith(index_name, "r_" + shard_kind);
        if (explicit_accessor) {
          // 1. extract tablename from index
          // remove basename_typeofuser from index_name
          // <basename_typeofuser/shardkind_tablename_accessorcol> e.g.
          // ref_doctors from ref_doctors_chat_colname know that colname has to
          // start with "_ACCESSOR" so find substring before it

          std::string beg = "r_" + shard_kind;
          std::string copy = index_name;
          copy.erase(0, beg.length() + 1);
          std::string accessor_table_name = copy.substr(0, copy.find("_"));
          std::cout << "ACCESSOR_TABLE_NAME: " << accessor_table_name
                    << std::endl;

          // 2. extract shardbycol
          std::string accessor_col_name = index_col;

          // 3. query this table (iterate over all values in the index that
          // correspond to the user) requesting the data with user_id

          // SELECT * FROM chat WHERE ACCESSOR_doctor_id = user_id AND
          // OWNER_patient_id in (index.value, ...)
          sqlast::Select accessor_stmt{accessor_table_name};
          // SELECT *
          accessor_stmt.AddColumn("*");

          // Left Hand Side: ACCESSOR_doctor_id = user_id
          std::unique_ptr<sqlast::BinaryExpression> doctor_equality =
              std::make_unique<sqlast::BinaryExpression>(
                  sqlast::Expression::Type::EQ);
          doctor_equality->SetLeft(
              std::make_unique<sqlast::ColumnExpression>(index_col));
          doctor_equality->SetRight(
              std::make_unique<sqlast::LiteralExpression>(user_id));

          // Right Hand Side: OWNER_patient_id in (index.value, ...)
          std::unique_ptr<sqlast::BinaryExpression> patient_in =
              std::make_unique<sqlast::BinaryExpression>(
                  sqlast::Expression::Type::IN);
          patient_in->SetLeft(
              std::make_unique<sqlast::ColumnExpression>(shard_by_access));
          MOVE_OR_RETURN(
              std::unordered_set<UserId> ids_set,
              index::LookupIndex(index_name, user_id, dataflow_state));
          std::vector<std::string> ids_vector;
          ids_vector.insert(ids_vector.end(), ids_set.begin(), ids_set.end());
          for (auto el : ids_vector) {
            std::cout << "ID: " << el << std::endl;
          }
          patient_in->SetRight(
              std::make_unique<sqlast::LiteralListExpression>(ids_vector));

          // combine both conditions into overall where condition of type AND
          std::unique_ptr<sqlast::BinaryExpression> where_condition =
              std::make_unique<sqlast::BinaryExpression>(
                  sqlast::Expression::Type::AND);
          std::unique_ptr<sqlast::Expression> doctor_equality_cast =
              static_cast<std::unique_ptr<sqlast::Expression>>(
                  std::move(doctor_equality));
          std::unique_ptr<sqlast::Expression> patient_in_cast =
              static_cast<std::unique_ptr<sqlast::Expression>>(
                  std::move(patient_in));
          where_condition->SetLeft(std::move(doctor_equality_cast));
          where_condition->SetRight(std::move(patient_in_cast));

          // set where condition
          accessor_stmt.SetWhereClause(std::move(where_condition));
          std::cout << "HERE7" << std::endl;

          MOVE_OR_RETURN(sql::SqlResult res,
                         select::Shard(accessor_stmt, state, dataflow_state));
          result.AddResultSet(res.NextResultSet());
        }
      }
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
