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

absl::StatusOr<sql::SqlResult> Get(const sqlast::GDPRStatement &stmt,
                                   Connection *connection) {
  const std::string &shard_kind = stmt.shard_kind();
  const std::string &user_id = stmt.user_id();
  auto &exec = connection->executor;

  shards::SharderState *state = connection->state->sharder_state();
  dataflow::DataFlowState *dataflow_state = connection->state->dataflow_state();
  SharedLock lock = state->ReaderLock();

  sql::SqlResult result(std::vector<sql::SqlResultSet>{});
  for (const auto &table_name : state->TablesInShard(shard_kind)) {
    dataflow::SchemaRef schema = dataflow_state->GetTableSchema(table_name);

    sql::SqlResult table_result(schema);
    for (const auto &info : state->GetShardingInformation(table_name)) {
      if (info.shard_kind != shard_kind) {
        continue;
      }

      // Augment returned results with the user_id.
      int aug_index = -1;
      if (!info.IsTransitive()) {
        aug_index = info.shard_by_index;
      }

      sqlast::Select tbl_stmt{info.sharded_table_name};
      tbl_stmt.AddColumn("*");
      table_result.Append(
          exec.Shard(&tbl_stmt, shard_kind, user_id, schema, aug_index), true);
    }
    result.AddResultSet(std::move(table_result.ResultSets().at(0)));
  }

  if (state->HasAccessorIndices(shard_kind)) {
    // Get all accessor indices that belong to this shard type
    const std::vector<AccessorIndexInformation> &accessor_indices =
        state->GetAccessorIndices(shard_kind);

    for (auto &accessor_index : accessor_indices) {
      // loop through every single accesor index type
      std::string accessor_table_name = accessor_index.table_name;
      std::string index_col = accessor_index.accessor_column_name;
      std::string index_name = accessor_index.index_name;
      std::string shard_by_access = accessor_index.shard_by_column_name;
      bool is_sharded = accessor_index.is_sharded;
      if (is_sharded) {
        dataflow::SchemaRef schema =
            dataflow_state->GetTableSchema(accessor_table_name);

        MOVE_OR_RETURN(std::unordered_set<UserId> ids_set,
                       index::LookupIndex(index_name, user_id, connection));

        // create select statement with equality check for each id
        sql::SqlResult table_result(schema);

        for (const auto &id : ids_set) {
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
              std::make_unique<sqlast::LiteralExpression>(user_id));

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

          // add to result
          table_result.Append(std::move(res), true);
        }
        result.AddResultSet(std::move(table_result.ResultSets().at(0)));
      } else {
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
            std::make_unique<sqlast::LiteralExpression>(user_id));

        // set where condition in select statement
        accessor_stmt.SetWhereClause(std::move(doctor_equality));

        // execute select
        MOVE_OR_RETURN(sql::SqlResult res,
                       select::Shard(accessor_stmt, connection, false));

        result.AddResultSet(std::move(res.ResultSets().at(0)));
      }
    }
  }

  return result;
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
