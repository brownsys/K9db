// GDPR statements handling (FORGET and GET).
#include "pelton/shards/sqlengine/gdpr.h"

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "pelton/shards/types.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/util/shard_name.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace gdpr {

namespace {

// add to result
// sql::SqlResult GetDataFromShardedTable(const ShardingInformation &info,
//                                        const std::string &unquoted_user_id,
//                                        const dataflow::SchemaRef &schema,
//                                        sql::PeltonExecutor *exec) {
//   int aug_index = -1;
//   if (!info.IsTransitive()) {
//     aug_index = info.shard_by_index;
//   }

//   sqlast::Select tbl_stmt{info.sharded_table_name};
//   tbl_stmt.AddColumn("*");
//   return exec->Shard(&tbl_stmt, unquoted_user_id, schema, aug_index);
// }

// absl::Status GetAccessableDataForAccessor(
//     const AccessorIndexInformation &accessor_index, const std::string &user_id,
//     Connection *connection, sql::SqlResult *result);

// absl::StatusOr<sql::SqlResult> GetAllMyValuesFromTable(
//     const std::string &shard_kind, const std::string &user_id,
//     const UnshardedTableName &table_name, Connection *connection) {
//   sql::SqlResult result(std::vector<sql::SqlResultSet>{});
//   const auto *state = connection->state->sharder_state();
//   if (state->HasAccessorIndices(shard_kind)) {
//     const std::vector<const AccessorIndexInformation *> acc_info =
//         state->GetAccessorInformationFor(shard_kind, table_name);
//     if (acc_info.size() > 0) {
//       for (const auto *info : acc_info) {
//         CHECK_STATUS(
//             GetAccessableDataForAccessor(*info, user_id, connection, &result));
//       }
//     }
//   }
//   if (state->IsSharded(table_name)) {
//     const std::vector<const ShardingInformation *> shard_info =
//         state->GetShardingInformationFor(table_name, shard_kind);
//     if (shard_info.size() > 0) {
//       for (const auto *info : shard_info) {
//         dataflow::SchemaRef schema =
//             connection->state->dataflow_state()->GetTableSchema(table_name);
//         result.AddResultSet(
//             std::move(GetDataFromShardedTable(*info, Dequote(user_id), schema,
//                                               &connection->executor)
//                           .ResultSets()
//                           .front()));
//       }
//     }
//   }
//   return result;
// }

// absl::StatusOr<std::vector<std::string>> GetLookupValues(
//     const std::string shard_kind, const std::string &user_id,
//     const std::optional<UnshardedTableName> &table_name,
//     Connection *connection) {
//   std::vector<std::string> lookup_values;
//   if (!table_name) {
//     lookup_values.push_back(user_id);
//   } else {
//     MOVE_OR_RETURN(
//         sql::SqlResult result,
//         GetAllMyValuesFromTable(shard_kind, user_id, *table_name, connection));
//     if (result.IsQuery() && result.Success()) {
//       auto &resultsets = result.ResultSets();
//       const auto &schema = resultsets.front().schema();
//       const auto &keys = schema.keys();
//       if (keys.size() != 1)
//         LOG(FATAL) << "Too many keys for " << shard_kind << " " << user_id
//                    << " " << (table_name ? *table_name : "self") << std::endl
//                    << schema;
//       const auto &key = keys.front();
//       for (auto &rset : resultsets) {
//         auto v = rset.Vec();
//         for (const auto &record : v) {
//           lookup_values.push_back(record.GetValue(key).GetSqlString());
//         }
//       }
//     }
//   }
//   return lookup_values;
// }

// absl::Status GetAccessableDataForAccessor(
//     const AccessorIndexInformation &accessor_index, const std::string &user_id,
//     Connection *connection, sql::SqlResult *result) {
//   std::string accessor_table_name = accessor_index.table_name;
//   std::string index_col = accessor_index.accessor_column_name;
//   std::string index_name = accessor_index.index_name;
//   std::string shard_by_access = accessor_index.shard_by_column_name;
//   bool is_sharded = accessor_index.is_sharded;

//   LOG(INFO) << "Handling accessor for " << accessor_table_name;

//   MOVE_OR_RETURN(const std::vector<std::string> lookup_values,
//                  GetLookupValues(accessor_index.shard_kind, user_id,
//                                  accessor_index.foreign_table, connection));
//   for (const auto &v : lookup_values) {
//     LOG(INFO) << v;
//   }
//   if (is_sharded) {
//     MOVE_OR_RETURN(std::unordered_set<UserId> ids_set,
//                    index::LookupIndex(index_name, user_id, connection));

//     // create select statement with equality check for each id

//     for (const auto &id : ids_set) {
//       for (const auto &val : lookup_values) {
//         sqlast::Select accessor_stmt{accessor_table_name};
//         // SELECT *
//         accessor_stmt.AddColumn("*");

//         // LHS: ACCESSOR_col = user_id
//         std::unique_ptr<sqlast::BinaryExpression> doctor_equality =
//             std::make_unique<sqlast::BinaryExpression>(
//                 sqlast::Expression::Type::EQ);
//         doctor_equality->SetLeft(
//             std::make_unique<sqlast::ColumnExpression>(index_col));
//         doctor_equality->SetRight(
//             std::make_unique<sqlast::LiteralExpression>(val));

//         // RHS: OWNER_col in (index.value, ...)
//         // OWNER_col == index.value
//         std::unique_ptr<sqlast::BinaryExpression> patient_equality =
//             std::make_unique<sqlast::BinaryExpression>(
//                 sqlast::Expression::Type::EQ);
//         patient_equality->SetLeft(
//             std::make_unique<sqlast::ColumnExpression>(shard_by_access));
//         patient_equality->SetRight(
//             std::make_unique<sqlast::LiteralExpression>(id));

//         // combine both conditions into overall where condition of type
//         // AND
//         std::unique_ptr<sqlast::BinaryExpression> where_condition =
//             std::make_unique<sqlast::BinaryExpression>(
//                 sqlast::Expression::Type::AND);
//         where_condition->SetLeft(
//             std::move(static_cast<std::unique_ptr<sqlast::Expression>>(
//                 std::move(doctor_equality))));
//         where_condition->SetRight(
//             std::move(static_cast<std::unique_ptr<sqlast::Expression>>(
//                 std::move(patient_equality))));

//         // set where condition in select statement
//         accessor_stmt.SetWhereClause(std::move(where_condition));

//         // execute select
//         MOVE_OR_RETURN(sql::SqlResult res,
//                        select::Shard(accessor_stmt, connection, false));

//         CHECK_EQ(res.ResultSets().size(), 1);
//         result->AddResultSet(std::move(res.ResultSets().front()));
//       }
//     }
//   } else {
//     for (const auto &val : lookup_values) {
//       sqlast::Select accessor_stmt{accessor_table_name};
//       // SELECT *
//       accessor_stmt.AddColumn("*");

//       // LHS: ACCESSOR_col = user_id
//       std::unique_ptr<sqlast::BinaryExpression> doctor_equality =
//           std::make_unique<sqlast::BinaryExpression>(
//               sqlast::Expression::Type::EQ);
//       doctor_equality->SetLeft(
//           std::make_unique<sqlast::ColumnExpression>(index_col));
//       doctor_equality->SetRight(
//           std::make_unique<sqlast::LiteralExpression>(val));

//       // set where condition in select statement
//       accessor_stmt.SetWhereClause(std::move(doctor_equality));

//       // execute select
//       MOVE_OR_RETURN(sql::SqlResult res,
//                      select::Shard(accessor_stmt, connection, false));

//       // add to result
//       result->AddResultSet(std::move(res.ResultSets().at(0)));
//     }
//   }
//   LOG(INFO) << "GetAccessableData done ";
//   return absl::OkStatus();
// }

// absl::Status GetAccessableData(const std::string &shard_kind,
//                                const std::string &user_id,
//                                Connection *connection, sql::SqlResult *result) {
//   // Get all accessor indices that belong to this shard type
//   const std::vector<AccessorIndexInformation> &accessor_indices =
//       connection->state->sharder_state()->GetAccessorIndices(shard_kind);

//   for (auto &accessor_index : accessor_indices) {
//     // loop through every single accesor index type
//     CHECK_STATUS(GetAccessableDataForAccessor(accessor_index, user_id,
//                                               connection, result));
//   }
//   return absl::OkStatus();
// }

absl::StatusOr<sql::SqlResult> Forget(const sqlast::GDPRStatement &stmt,
                                      Connection *connection) {
  const std::string &shard_kind = stmt.shard_kind();
  const std::string &user_id = stmt.user_id();

  shards::SharderState &state = connection->state->SharderState();
  dataflow::DataFlowState &dataflow_state = connection->state->DataflowState();
  // UniqueLock lock = state->WriterLock();

  shards::Shard current_shard = state.GetShard(shard_kind);
  sql::SqlResult result(static_cast<int>(0));

  for (const auto &table_name : current_shard.owned_tables) {
    bool update_flows = dataflow_state.HasFlowsFor(table_name);
    std::vector<dataflow::Record> dataflow_updates;

    sql::SqlResultSet del_set = 
      connection->state->Database()->DeleteShard(table_name, util::ShardName(shard_kind, user_id));

    for (const auto &row : del_set.rows()) {
      std::cout << row << std::endl;
    }

    result.Append(sql::SqlResult(static_cast<int>(del_set.size())), true);

    // Delete was successful, time to update dataflows.
    if (update_flows) {
      std::vector<dataflow::Record> vec = del_set.Vec();
      std::vector<dataflow::Value> pks;
      for (const auto &r : vec) {
        size_t pk_col_index = r.schema().keys().at(0);
        pks.push_back(r.GetValue(pk_col_index));
      }

      // Count how many owners each record has.
      std::vector<size_t> counts = connection->state->Database()->CountShards(table_name, pks);
      assert(vec.size() == counts.size());
      for (size_t i=0; i < vec.size(); i++) {
        if (!counts.at(i)) {
          dataflow_updates.push_back(std::move(vec.at(i)));
        }
      }
    }
    
    // Update dataflow with deletions from table.
    dataflow_state.ProcessRecords(table_name, std::move(dataflow_updates));
  }

  // // Anonymize when accessor deletes their data
  // if (state->HasAccessorIndices(shard_kind)) {
  //   // Get all accessor indices that belong to this shard type
  //   const std::vector<AccessorIndexInformation> &accessor_indices =
  //       state->GetAccessorIndices(shard_kind);

  //   for (auto &accessor_index : accessor_indices) {
  //     std::string accessor_table_name = accessor_index.table_name;
  //     std::string index_col = accessor_index.accessor_column_name;
  //     std::string index_name = accessor_index.index_name;
  //     std::string shard_by_access = accessor_index.shard_by_column_name;

  //     bool is_sharded = accessor_index.is_sharded;
  //     for (auto &[anon_col_name, anon_col_type] :
  //          accessor_index.anonymize_columns) {
  //       if (is_sharded) {
  //         MOVE_OR_RETURN(std::unordered_set<UserId> ids_set,
  //                        index::LookupIndex(index_name, user_id, connection));

  //         // create update statement with equality check for each id
  //         for (const auto &id : ids_set) {
  //           sqlast::Update anonymize_stmt{accessor_table_name};

  //           // UPDATE ACCESSOR_col = ANONYMIZED WHERE ACCESSOR_ = user_id

  //           // ACCESSOR_col = ANONYMIZED
  //           std::string anonymized_value = "";
  //           switch (anon_col_type) {
  //             case sqlast::ColumnDefinition::Type::UINT:
  //               anonymized_value = "999999";
  //               break;
  //             case sqlast::ColumnDefinition::Type::INT:
  //               anonymized_value = "-999999";
  //               break;
  //             case sqlast::ColumnDefinition::Type::TEXT:
  //               anonymized_value = "\"ANONYMIZED\"";
  //               break;
  //             case sqlast::ColumnDefinition::Type::DATETIME:
  //               anonymized_value = "\"ANONYMIZED\"";
  //               break;
  //           }
  //           anonymize_stmt.AddColumnValue(anon_col_name, anonymized_value);

  //           std::unique_ptr<sqlast::BinaryExpression> where =
  //               std::make_unique<sqlast::BinaryExpression>(
  //                   sqlast::Expression::Type::EQ);
  //           where->SetLeft(
  //               std::make_unique<sqlast::ColumnExpression>(index_col));
  //           where->SetRight(
  //               std::make_unique<sqlast::LiteralExpression>(user_id));

  //           // set where condition in update statement
  //           anonymize_stmt.SetWhereClause(std::move(where));

  //           // execute update
  //           MOVE_OR_RETURN(sql::SqlResult res,
  //                          update::Shard(anonymize_stmt, connection, false));

  //           // add to result
  //           result.Append(std::move(res), true);
  //         }
  //       } else {
  //         // anonymizing update statement for non-sharded schemas
  //         sqlast::Update anonymize_stmt{accessor_table_name};

  //         // UPDATE ANONYMIZE_col = ANONYMIZED WHERE ANONYMIZE_col = user_id

  //         // ANONYMIZE_col = ANONYMIZED
  //         std::string anonymized_value = "";
  //         switch (anon_col_type) {
  //           case sqlast::ColumnDefinition::Type::UINT:
  //             anonymized_value = "999999";
  //             break;
  //           case sqlast::ColumnDefinition::Type::INT:
  //             anonymized_value = "-999999";
  //             break;
  //           case sqlast::ColumnDefinition::Type::TEXT:
  //             anonymized_value = "\"ANONYMIZED\"";
  //             break;
  //           case sqlast::ColumnDefinition::Type::DATETIME:
  //             anonymized_value = "\"ANONYMIZED\"";
  //             break;
  //         }
  //         anonymize_stmt.AddColumnValue(anon_col_name, anonymized_value);

  //         std::unique_ptr<sqlast::BinaryExpression> where =
  //             std::make_unique<sqlast::BinaryExpression>(
  //                 sqlast::Expression::Type::EQ);
  //         where->SetLeft(std::make_unique<sqlast::ColumnExpression>(index_col));
  //         where->SetRight(std::make_unique<sqlast::LiteralExpression>(user_id));

  //         // set where condition in update statement
  //         anonymize_stmt.SetWhereClause(std::move(where));

  //         // execute update
  //         MOVE_OR_RETURN(sql::SqlResult res,
  //                        update::Shard(anonymize_stmt, connection, false));

  //         // add to result
  //         result.Append(std::move(res), true);
  //       }
  //     }
  //   }
  // }
  return result;
}

absl::StatusOr<sql::SqlResult> Get(const sqlast::GDPRStatement &stmt,
                                   Connection *connection) {
  const std::string &shard_kind = stmt.shard_kind();
  const std::string &user_id = stmt.user_id();

  shards::SharderState &state = connection->state->SharderState();
  // UniqueLock lock = state->WriterLock();

  shards::Shard current_shard = state.GetShard(shard_kind);
  std::vector<sql::SqlResultSet> result;
  std::unordered_map<shards::TableName, std::vector<sql::SqlResultSet>> result_map;

  for (const auto &table_name : current_shard.owned_tables) {
    // Get data from tables owned by user.
    sql::SqlResultSet get_set = 
      connection->state->Database()->GetShard(table_name, util::ShardName(shard_kind, user_id));

    for (const auto &row : get_set.rows()) {
      std::cout << row << std::endl;
    }
    result_map[table_name].push_back(std::move(get_set));

    // Get data from tables accessed by user.
    for (const auto &accessed_table : state.GetTable(table_name).access_dependents) {
      ColumnName accessor_column;
      if (accessed_table.second->type == shards::InfoType::DIRECT) {
        DirectInfo info = std::get<DirectInfo>(accessed_table.second->info);
        accessor_column = info.column;
      } else if (accessed_table.second->type == shards::InfoType::TRANSITIVE) {
        TransitiveInfo info = std::get<TransitiveInfo>(accessed_table.second->info);
        accessor_column = info.column;
      } else if (accessed_table.second->type == shards::InfoType::VARIABLE) {
        VariableInfo info = std::get<VariableInfo>(accessed_table.second->info);
        accessor_column = info.column;
      }

      sqlast::Select accessor_stmt{accessed_table.first};
      // SELECT *
      accessor_stmt.AddColumn("*");

      // [accessor_column] = [user_id]
      std::unique_ptr<sqlast::BinaryExpression> accessor_equality =
          std::make_unique<sqlast::BinaryExpression>(
              sqlast::Expression::Type::EQ);
      accessor_equality->SetLeft(
          std::make_unique<sqlast::ColumnExpression>(accessor_column));
      accessor_equality->SetRight(
          std::make_unique<sqlast::LiteralExpression>(user_id));

      // set where condition in select statement
      accessor_stmt.SetWhereClause(std::move(accessor_equality));

      std::cout << "[CONSTRUCTED STATEMENT]: " << accessor_stmt << std::endl;

      util::SharedLock lock = connection->state->ReaderLock();
      SelectContext context(accessor_stmt, connection, &lock);

      MOVE_OR_RETURN(sql::SqlResult accessor_result, context.Exec());
      for (auto &rs : accessor_result.ResultSets()) {
        for (const auto &row : rs.rows()) {
          std::cout << row << std::endl;
        }
        result_map[accessed_table.first].push_back(std::move(rs));
      }
    }
  }

  // Squash results in result_map into SqlResult result.
  for (auto &[_, sets] : result_map) {
    if (sets.size() > 0) {
      sql::SqlResultSet result_set(sets.at(0).schema());
      for (sql::SqlResultSet &rs : sets) {
        result_set.Append(std::move(rs), false);
      }
      result.push_back(std::move(result_set));
    }
  }

  return sql::SqlResult(std::move(result));
}

}  // namespace

absl::StatusOr<sql::SqlResult> Shard(const sqlast::GDPRStatement &stmt,
                                     Connection *connection) {
  if (stmt.operation() == sqlast::GDPRStatement::Operation::FORGET) {
    return Forget(stmt, connection);
  } else {
    return Get(stmt, connection);
  }
}

}  // namespace gdpr
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
