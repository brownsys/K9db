// GDPR statements handling (FORGET and GET).
#include "pelton/shards/sqlengine/gdpr.h"

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/shards/sqlengine/update.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/shards/upgradable_lock.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace gdpr {

namespace {

// add to result
sql::SqlResult GetDataFromShardedTable(const ShardingInformation &info,
                                       const std::string &unquoted_user_id,
                                       const dataflow::SchemaRef &schema,
                                       sql::PeltonExecutor *exec) {
  int aug_index = -1;
  if (!info.IsTransitive()) {
    aug_index = info.shard_by_index;
  }

  sqlast::Select tbl_stmt{info.sharded_table_name};
  tbl_stmt.AddColumn("*");
  return exec->Shard(&tbl_stmt, info.shard_kind, unquoted_user_id, schema,
                    aug_index);
}

absl::Status GetAccessableDataForAccessor(
    const AccessorIndexInformation &accessor_index, const std::string &user_id,
    Connection *connection, sql::SqlResult *result);

absl::StatusOr<sql::SqlResult> GetAllMyValuesFromTable(
    const std::string &shard_kind, const std::string &user_id,
    const UnshardedTableName &table_name, Connection *connection) {
  sql::SqlResult result(std::vector<sql::SqlResultSet>{});
  const auto *state = connection->state->sharder_state();
  if (state->HasAccessorIndices(shard_kind)) {
    const std::vector<const AccessorIndexInformation *> acc_info =
        state->GetAccessorInformationFor(shard_kind, table_name);
    if (acc_info.size() > 0) {
      for (const auto *info : acc_info) {
        CHECK_STATUS(
            GetAccessableDataForAccessor(*info, user_id, connection, &result));
      }
    }
  }
  if (state->IsSharded(table_name)) {
    const std::vector<const ShardingInformation *> shard_info =
        state->GetShardingInformationFor(table_name, shard_kind);
    if (shard_info.size() > 0) {
      for (const auto *info : shard_info) {
        dataflow::SchemaRef schema =
            connection->state->dataflow_state()->GetTableSchema(table_name);
        result.AddResultSet(
            std::move(GetDataFromShardedTable(*info, Dequote(user_id), schema,
                                              &connection->executor)
                          .ResultSets()
                          .front()));
      }
    }
  }
  return result;
}

absl::StatusOr<std::vector<std::string>> GetLookupValues(
    const std::string shard_kind, const std::string &user_id,
    const std::optional<UnshardedTableName> &table_name,
    Connection *connection) {
  std::vector<std::string> lookup_values;
  if (!table_name) {
    lookup_values.push_back(user_id);
  } else {
    MOVE_OR_RETURN(
        sql::SqlResult result,
        GetAllMyValuesFromTable(shard_kind, user_id, *table_name, connection));
    if (result.IsQuery() && result.Success()) {
      auto &resultsets = result.ResultSets();
      const auto &schema = resultsets.front().schema();
      const auto &keys = schema.keys();
      if (keys.size() != 1)
        LOG(FATAL) << "Too many keys for " << shard_kind << " " << user_id
                   << " " << (table_name ? *table_name : "self") << std::endl
                   << schema;
      const auto &key = keys.front();
      for (auto &rset : resultsets) {
        auto v = rset.Vec();
        for (const auto &record : v) {
          lookup_values.push_back(record.GetValue(key).GetSqlString());
        }
      }
    }
  }
  return lookup_values;
}

absl::Status GetAccessableDataForAccessor(
    const AccessorIndexInformation &accessor_index, const std::string &user_id,
    Connection *connection, sql::SqlResult *result) {
  dataflow::DataFlowState *dataflow_state = connection->state->dataflow_state();
  shards::SharderState *state = connection->state->sharder_state();
  std::string accessor_table_name = accessor_index.table_name;
  std::string index_col = accessor_index.accessor_column_name;
  std::string index_name = accessor_index.index_name;
  std::string shard_by_access = accessor_index.shard_by_column_name;
  bool is_sharded = accessor_index.is_sharded;

  LOG(INFO) << "Handling accessor for " << accessor_table_name;

  MOVE_OR_RETURN(const std::vector<std::string> lookup_values,
                 GetLookupValues(accessor_index.shard_kind, user_id,
                                 accessor_index.foreign_table, connection));
  for (const auto &v : lookup_values) {
    LOG(INFO) << v;
  }
  if (is_sharded) {
    MOVE_OR_RETURN(std::unordered_set<UserId> ids_set,
                   index::LookupIndex(index_name, user_id, connection));

    // create select statement with equality check for each id

    for (const auto &id : ids_set) {
      for (const auto &val : lookup_values) {
        sqlast::Select accessor_stmt{accessor_table_name};
        // SELECT *
        accessor_stmt.AddColumn("*");

        // LHS: ACCESSOR_col = user_id
        std::unique_ptr<sqlast::BinaryExpression> doctor_equality =
            std::make_unique<sqlast::BinaryExpression>(
                sqlast::Expression::Type::EQ);
        doctor_equality->SetLeft(
            std::make_unique<sqlast::ColumnExpression>(index_col));
        doctor_equality->SetRight(
            std::make_unique<sqlast::LiteralExpression>(val));

        // RHS: OWNER_col in (index.value, ...)
        // OWNER_col == index.value
        std::unique_ptr<sqlast::BinaryExpression> patient_equality =
            std::make_unique<sqlast::BinaryExpression>(
                sqlast::Expression::Type::EQ);
        patient_equality->SetLeft(
            std::make_unique<sqlast::ColumnExpression>(shard_by_access));
        patient_equality->SetRight(
            std::make_unique<sqlast::LiteralExpression>(id));

        // combine both conditions into overall where condition of type
        // AND
        std::unique_ptr<sqlast::BinaryExpression> where_condition =
            std::make_unique<sqlast::BinaryExpression>(
                sqlast::Expression::Type::AND);
        where_condition->SetLeft(
            std::move(static_cast<std::unique_ptr<sqlast::Expression>>(
                std::move(doctor_equality))));
        where_condition->SetRight(
            std::move(static_cast<std::unique_ptr<sqlast::Expression>>(
                std::move(patient_equality))));

        // set where condition in select statement
        accessor_stmt.SetWhereClause(std::move(where_condition));

        // execute select
        MOVE_OR_RETURN(sql::SqlResult res,
                       select::Shard(accessor_stmt, connection, false));

        CHECK_EQ(res.ResultSets().size(), 1);
        result->AddResultSet(std::move(res.ResultSets().front()));
      }
    }
  } else {
    for (const auto &val : lookup_values) {
      sqlast::Select accessor_stmt{accessor_table_name};
      // SELECT *
      accessor_stmt.AddColumn("*");

      // LHS: ACCESSOR_col = user_id
      std::unique_ptr<sqlast::BinaryExpression> doctor_equality =
          std::make_unique<sqlast::BinaryExpression>(
              sqlast::Expression::Type::EQ);
      doctor_equality->SetLeft(
          std::make_unique<sqlast::ColumnExpression>(index_col));
      doctor_equality->SetRight(
          std::make_unique<sqlast::LiteralExpression>(val));

      // set where condition in select statement
      accessor_stmt.SetWhereClause(std::move(doctor_equality));

      // execute select
      MOVE_OR_RETURN(sql::SqlResult res,
                     select::Shard(accessor_stmt, connection, false));

      // add to result
      result->AddResultSet(std::move(res.ResultSets().at(0)));
    }
  }
  LOG(INFO) << "GetAccessableData done ";
  return absl::OkStatus();
}

absl::Status GetAccessableData(const std::string &shard_kind,
                               const std::string &user_id,
                               Connection *connection, sql::SqlResult *result) {
  // Get all accessor indices that belong to this shard type
  const std::vector<AccessorIndexInformation> &accessor_indices =
      connection->state->sharder_state()->GetAccessorIndices(shard_kind);

  for (auto &accessor_index : accessor_indices) {
    // loop through every single accesor index type
    CHECK_STATUS(GetAccessableDataForAccessor(accessor_index, user_id,
                                              connection, result));
  }
  return absl::OkStatus();
}

absl::StatusOr<sql::SqlResult> Forget(const sqlast::GDPRStatement &stmt,
                                      Connection *connection) {
  const std::string &shard_kind = stmt.shard_kind();
  const std::string &user_id = stmt.user_id();
  auto &exec = connection->executor;

  // XXX(malte): if we properly removed the shared on GDPR FORGET below, this
  // would need to be a unique_lock, as it would change sharder state. However,
  // the current code does not update the sharder state AFAICT.
  shards::SharderState *state = connection->state->sharder_state();
  dataflow::DataFlowState *dataflow_state = connection->state->dataflow_state();
  UniqueLock lock = state->WriterLock();

  sql::SqlResult result(static_cast<int>(0));
  for (const auto &table_name : state->TablesInShard(shard_kind)) {
    bool update_flows = dataflow_state->HasFlowsFor(table_name);
    dataflow::SchemaRef schema = dataflow_state->GetTableSchema(table_name);

    sql::SqlResult table_result(static_cast<int>(0));
    if (update_flows) {
      table_result = sql::SqlResult(schema);
    }
    for (const auto &info : state->GetShardingInformation(table_name)) {
      if (info.shard_kind != shard_kind) {
        continue;
      }

      // Augment returned results with the user_id.
      int aug_index = -1;
      if (!info.IsTransitive()) {
        aug_index = info.shard_by_index;
      }

      // XXX(malte): need to upgrade SharderStateLock here in the future.
      sqlast::Delete tbl_stmt{info.sharded_table_name, update_flows};
      table_result.Append(
          exec.Shard(&tbl_stmt, shard_kind, user_id, schema, aug_index), true);
    }

    // Delete was successful, time to update dataflows.
    if (update_flows) {
      std::vector<dataflow::Record> records =
          table_result.ResultSets().at(0).Vec();
      result.Append(sql::SqlResult(records.size()), true);
      dataflow_state->ProcessRecords(table_name, std::move(records));
    } else {
      result.Append(std::move(table_result), true);
    }
  }

  // Anonymize when accessor deletes their data
  if (state->HasAccessorIndices(shard_kind)) {
    // Get all accessor indices that belong to this shard type
    const std::vector<AccessorIndexInformation> &accessor_indices =
        state->GetAccessorIndices(shard_kind);

    for (auto &accessor_index : accessor_indices) {
      std::string accessor_table_name = accessor_index.table_name;
      std::string index_col = accessor_index.accessor_column_name;
      std::string index_name = accessor_index.index_name;
      std::string shard_by_access = accessor_index.shard_by_column_name;

      bool is_sharded = accessor_index.is_sharded;
      for (auto &[anon_col_name, anon_col_type] :
           accessor_index.anonymize_columns) {
        if (is_sharded) {
          MOVE_OR_RETURN(std::unordered_set<UserId> ids_set,
                         index::LookupIndex(index_name, user_id, connection));

          // create update statement with equality check for each id
          for (const auto &id : ids_set) {
            sqlast::Update anonymize_stmt{accessor_table_name};

            // UPDATE ACCESSOR_col = ANONYMIZED WHERE ACCESSOR_ = user_id

            // ACCESSOR_col = ANONYMIZED
            std::string anonymized_value = "";
            switch (anon_col_type) {
              case sqlast::ColumnDefinition::Type::UINT:
                anonymized_value = "999999";
                break;
              case sqlast::ColumnDefinition::Type::INT:
                anonymized_value = "-999999";
                break;
              case sqlast::ColumnDefinition::Type::TEXT:
                anonymized_value = "\"ANONYMIZED\"";
                break;
              case sqlast::ColumnDefinition::Type::DATETIME:
                anonymized_value = "\"ANONYMIZED\"";
                break;
            }
            anonymize_stmt.AddColumnValue(anon_col_name, anonymized_value);

            std::unique_ptr<sqlast::BinaryExpression> where =
                std::make_unique<sqlast::BinaryExpression>(
                    sqlast::Expression::Type::EQ);
            where->SetLeft(
                std::make_unique<sqlast::ColumnExpression>(index_col));
            where->SetRight(
                std::make_unique<sqlast::LiteralExpression>(user_id));

            // set where condition in update statement
            anonymize_stmt.SetWhereClause(std::move(where));

            // execute update
            MOVE_OR_RETURN(sql::SqlResult res,
                           update::Shard(anonymize_stmt, connection, false));

            // add to result
            result.Append(std::move(res), true);
          }
        } else {
          // anonymizing update statement for non-sharded schemas
          sqlast::Update anonymize_stmt{accessor_table_name};

          // UPDATE ANONYMIZE_col = ANONYMIZED WHERE ANONYMIZE_col = user_id

          // ANONYMIZE_col = ANONYMIZED
          std::string anonymized_value = "";
          switch (anon_col_type) {
            case sqlast::ColumnDefinition::Type::UINT:
              anonymized_value = "999999";
              break;
            case sqlast::ColumnDefinition::Type::INT:
              anonymized_value = "-999999";
              break;
            case sqlast::ColumnDefinition::Type::TEXT:
              anonymized_value = "\"ANONYMIZED\"";
              break;
            case sqlast::ColumnDefinition::Type::DATETIME:
              anonymized_value = "\"ANONYMIZED\"";
              break;
          }
          anonymize_stmt.AddColumnValue(anon_col_name, anonymized_value);

          std::unique_ptr<sqlast::BinaryExpression> where =
              std::make_unique<sqlast::BinaryExpression>(
                  sqlast::Expression::Type::EQ);
          where->SetLeft(std::make_unique<sqlast::ColumnExpression>(index_col));
          where->SetRight(std::make_unique<sqlast::LiteralExpression>(user_id));

          // set where condition in update statement
          anonymize_stmt.SetWhereClause(std::move(where));

          // execute update
          MOVE_OR_RETURN(sql::SqlResult res,
                         update::Shard(anonymize_stmt, connection, false));

          // add to result
          result.Append(std::move(res), true);
        }
      }
    }
  }
  return result;
}
void Print(sql::SqlResult &&result, bool print) {
  if (print) {
    if (result.IsStatement()) {
      std::cout << "Success: " << result.Success() << std::endl;
    } else if (result.IsUpdate()) {
      std::cout << "Affected rows: " << result.UpdateCount() << std::endl;
    } else if (result.IsQuery()) {
      for (sql::SqlResultSet &resultset : result.ResultSets()) {
        std::cout << std::endl;
        std::cout << resultset.schema() << std::endl;
        std::vector<dataflow::Record> records = resultset.Vec();
        for (dataflow::Record &record : records) {
          std::cout << record << std::endl;
        }
      }
    }
  }
}

absl::StatusOr<sql::SqlResult> Get(const sqlast::GDPRStatement &stmt,
                                   Connection *connection) {
  const std::string &shard_kind = stmt.shard_kind();
  const std::string &user_id = stmt.user_id();
  const std::string &unquoted_user_id = Dequote(user_id);
  auto &exec = connection->executor;

  shards::SharderState *state = connection->state->sharder_state();
  dataflow::DataFlowState *dataflow_state = connection->state->dataflow_state();
  SharedLock lock = state->ReaderLock();

  sql::SqlResult result(std::vector<sql::SqlResultSet>{});
  for (const auto &table_name : state->TablesInShard(shard_kind)) {
    if (!state->ShardExists(shard_kind, unquoted_user_id))
      return absl::InvalidArgumentError("Shard for user " + unquoted_user_id +
                                        " does not exist");
    dataflow::SchemaRef schema = dataflow_state->GetTableSchema(table_name);

    sql::SqlResult table_result(schema);
    for (const ShardingInformation *info :
         state->GetShardingInformationFor(table_name, shard_kind)) {
      // Augment returned results with the user_id.
      int aug_index = -1;
      if (!info->IsTransitive()) {
        aug_index = info->shard_by_index;
      }

      LOG(INFO) << "Looking up from table " << info->sharded_table_name;

      sqlast::Select tbl_stmt{info->sharded_table_name};
      tbl_stmt.AddColumn("*");
      table_result.Append(exec.Shard(&tbl_stmt, shard_kind, unquoted_user_id,
                                     schema, aug_index),
                          true);
    }
    CHECK_EQ(table_result.ResultSets().size(), 1);
    LOG(INFO) << "Found a total of " << table_result.ResultSets().front().size()
              << " rows";
    result.AddResultSet(std::move(table_result.ResultSets().front()));
  }

  if (state->HasAccessorIndices(shard_kind)) {
    CHECK_STATUS(GetAccessableData(shard_kind, user_id, connection, &result));
  }
  return result;
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
