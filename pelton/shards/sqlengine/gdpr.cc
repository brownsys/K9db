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

std::pair<ColumnName, ColumnIndex> GetAccessorNameAccessedIndex(
                                    shards::ShardDescriptor *accessed) {
  ColumnName accessor_column;
  ColumnIndex accessed_column_index;

  if (accessed->type == shards::InfoType::DIRECT) {
    DirectInfo info = std::get<DirectInfo>(accessed->info);
    accessor_column = info.column;
    accessed_column_index = info.next_column_index;
  } else if (accessed->type == shards::InfoType::TRANSITIVE) {
    TransitiveInfo info = std::get<TransitiveInfo>(accessed->info);
    accessor_column = info.column;
    accessed_column_index = info.next_column_index;
  } else if (accessed->type == shards::InfoType::VARIABLE) {
    VariableInfo info = std::get<VariableInfo>(accessed->info);
    accessor_column = info.column;
    accessed_column_index = info.origin_column_index;
  }

  return std::make_pair(accessor_column, accessed_column_index);
}

auto GetAccessedData(auto accessed_queue, Connection *connection) {
  std::unordered_map<shards::TableName, std::vector<sql::SqlResultSet>> result_map;
  shards::SharderState &state = connection->state->SharderState();

  // Iterate over all access dependents via BFS.
  while (!accessed_queue.empty()) {
    std::pair<std::pair<shards::TableName, shards::ShardDescriptor *>, 
              std::vector<std::string>> next_accessed = accessed_queue.front();
    std::pair<shards::TableName, shards::ShardDescriptor *> accessed_table = next_accessed.first;
    std::vector<std::string> accessor_table_ids = next_accessed.second;
    accessed_queue.pop();

    auto accessor_name_accessed_idx = GetAccessorNameAccessedIndex(accessed_table.second);
    ColumnName accessor_column = accessor_name_accessed_idx.first;

    sqlast::Select accessor_stmt{accessed_table.first};
    
    // SELECT *
    accessor_stmt.AddColumn("*");

    // [accessor_column] IN (accessor_table_ids)
    std::unique_ptr<sqlast::BinaryExpression> accessor_equality =
        std::make_unique<sqlast::BinaryExpression>(
            sqlast::Expression::Type::IN);
    accessor_equality->SetLeft(
        std::make_unique<sqlast::ColumnExpression>(accessor_column));
    accessor_equality->SetRight(
        std::make_unique<sqlast::LiteralListExpression>(accessor_table_ids));

    // Set where condition in select statement.
    accessor_stmt.SetWhereClause(std::move(accessor_equality));

    // Print the SELECT statement before executing.
    std::cout << "[CONSTRUCTED STATEMENT]: " << accessor_stmt << std::endl;

    util::SharedLock lock = connection->state->ReaderLock();
    SelectContext context(accessor_stmt, connection, &lock);

    MOVE_OR_PANIC(sql::SqlResult accessor_result, context.Exec());

    // Push next layer of access_dependents into the queue.
    for (const auto &next_accessed_table : state.GetTable(accessed_table.first).access_dependents) {
      std::vector<std::string> next_accessor_table_ids;

      auto accessor_name_accessed_idx = GetAccessorNameAccessedIndex(next_accessed_table.second);
      ColumnIndex next_accessed_column_index =  accessor_name_accessed_idx.second;

      for (auto &rs : accessor_result.ResultSets()) {
        for (const auto &row : rs.rows()) {
          next_accessor_table_ids.push_back(row.GetValue(next_accessed_column_index).GetSqlString());
        }
      }

      accessed_queue.push(std::make_pair(next_accessed_table, next_accessor_table_ids));
    }

    // Collect accessed data in result_map.
    for (auto &rs : accessor_result.ResultSets()) {
      result_map[accessed_table.first].push_back(std::move(rs));
    }
  }

  return result_map;
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

    std::queue<std::pair<std::pair<shards::TableName, shards::ShardDescriptor *>, 
               std::vector<std::string>>> accessed_queue;

    // Push first layer of access dependents into the accessed_queue.
    for (const auto &accessed_table : state.GetTable(table_name).access_dependents) {
      std::vector<std::string> accessor_table_ids;
      auto accessor_name_accessed_idx = GetAccessorNameAccessedIndex(accessed_table.second);
      ColumnName accessor_column = accessor_name_accessed_idx.first;
      ColumnIndex accessed_column_index =  accessor_name_accessed_idx.second;

      for (const auto &row : get_set.rows()) {
        accessor_table_ids.push_back(row.GetValue(accessed_column_index).GetSqlString());
      }

      accessed_queue.push(std::make_pair(accessed_table, accessor_table_ids));
    }

    // Collect owned data in result_map.
    result_map[table_name].push_back(std::move(get_set));

    // Collect accessed data in result_map.
    auto accessed_result_map = GetAccessedData(accessed_queue, connection);
    for (auto &[table_name, sets] : accessed_result_map) {
      for (sql::SqlResultSet &rs : sets) {
        result_map[table_name].push_back(std::move(rs));
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
