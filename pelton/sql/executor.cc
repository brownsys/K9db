// Intermediate layer between pelton and whatever underlying DB we use.
#include "pelton/sql/executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"
#include "pelton/sql/connections/rocksdb_connection.h"

namespace pelton {
namespace sql {

// Creates an empty SqlResult that corresponds to the given sql statement type.
SqlResult PeltonExecutor::EmptyResult(const sqlast::AbstractStatement *sql,
                                      const dataflow::SchemaRef &schema) {
  switch (sql->type()) {
    // Statement was handled successfully.
    case sqlast::AbstractStatement::Type::CREATE_TABLE:
    case sqlast::AbstractStatement::Type::CREATE_INDEX:
      return SqlResult(true);
    // 0 rows are updated.
    case sqlast::AbstractStatement::Type::UPDATE:
      return SqlResult(static_cast<int>(0));
    // Insert/Delete are usually updates but might have a "returning" component.
    case sqlast::AbstractStatement::Type::INSERT:
      if (static_cast<const sqlast::Insert *>(sql)->returning()) {
        return SqlResult(schema);
      }
      return SqlResult(static_cast<int>(0));
    case sqlast::AbstractStatement::Type::DELETE:
      if (static_cast<const sqlast::Delete *>(sql)->returning()) {
        return SqlResult(schema);
      }
      return SqlResult(static_cast<int>(0));
    // Empty result set.
    case sqlast::AbstractStatement::Type::SELECT:
      return SqlResult(schema);
    // Something else.
    default:
      LOG(FATAL) << "Unsupported SQL statement in PeltonExecutor";
  }
}

// Initialization: initialize the eager executor so that it maintains
// an open connection to the underlying DB.
void PeltonExecutor::Initialize(const std::string &db_name) {
  this->connection_ = std::make_unique<RocksdbConnection>(db_name);
}

// Execution of SQL statements.
// Execute statement against the default un-sharded database.
SqlResult PeltonExecutor::Default(const sqlast::AbstractStatement *sql,
                                  const dataflow::SchemaRef &schema) {
#ifndef PELTON_OPT
  LOG(INFO) << "Shard: default";
#endif
  return this->Execute(sql, schema, "", -1);
}

// Execute statement against given user shard.
SqlResult PeltonExecutor::Shard(const sqlast::AbstractStatement *sql,
                                const shards::UserId &user_id,
                                const dataflow::SchemaRef &schema,
                                int aug_index) {
#ifndef PELTON_OPT
  LOG(INFO) << "Shard: " << user_id;
#endif

  // Execute against the underlying shard.
  return this->Execute(sql, schema, user_id, aug_index);
}

// Execute statement against given user shards.
SqlResult PeltonExecutor::Shards(
    const sqlast::AbstractStatement *sql,
    const std::unordered_set<shards::UserId> &user_ids,
    const dataflow::SchemaRef &schema, int aug_index) {
  // If no user_ids are provided, we return an empty result.
  SqlResult result = EmptyResult(sql, schema);

  if (user_ids.size() > 5) {
    LOG(WARNING) << "Perf Warning: some query executes over more than 5 shards "
                 << user_ids.size() << " query " << *sql;
  }

  // This result set is a proxy that allows access to results from all shards.
  for (const shards::UserId &user_id : user_ids) {
    result.Append(this->Shard(sql, user_id, schema, aug_index), true);
  }

  return result;
}

// Actual SQL statement execution.
SqlResult PeltonExecutor::Execute(const sqlast::AbstractStatement *stmt,
                                  const dataflow::SchemaRef &schema,
                                  const shards::UserId &user_id,
                                  int aug_index) {
#ifndef PELTON_OPT
  LOG(INFO) << "Statement: " << stmt;
#endif

  switch (stmt->type()) {
    // Statements: return a boolean signifying success.
    case sqlast::AbstractStatement::Type::CREATE_TABLE:
    case sqlast::AbstractStatement::Type::CREATE_INDEX:
      return SqlResult(this->connection_->ExecuteStatement(stmt, user_id));

    // Updates: return a count of rows affected.
    case sqlast::AbstractStatement::Type::UPDATE: {
      auto [row_count, lid] = this->connection_->ExecuteUpdate(stmt, user_id);
      return SqlResult(row_count, lid);
    }

    // Insert and Delete might be returning.
    case sqlast::AbstractStatement::Type::INSERT: {
      if (!static_cast<const sqlast::Insert *>(stmt)->returning()) {
        auto [row_count, lid] = this->connection_->ExecuteUpdate(stmt, user_id);
        return SqlResult(row_count, lid);
      }
      break;
    }
    case sqlast::AbstractStatement::Type::DELETE:
      if (!static_cast<const sqlast::Delete *>(stmt)->returning()) {
        auto [row_count, lid] = this->connection_->ExecuteUpdate(stmt, user_id);
        return SqlResult(row_count, lid);
      }
      break;

    // Queries: return a (lazy) result set of records.
    case sqlast::AbstractStatement::Type::SELECT: {
      break;
    }

    default:
      LOG(FATAL) << "Unsupported SQL statement in PeltonExecutor";
  }

  // Must be a query (or a returning INSERT/DELETE).
  // Set up augmentation information to augment any sharding columns to the
  // result set.
  std::vector<AugInfo> aug_info;
  if (aug_index > -1) {
    aug_info.push_back({static_cast<size_t>(aug_index), user_id});
  }
  // Create a result set for output.
  SqlResultSet resultset =
      this->connection_->ExecuteQuery(stmt, schema, aug_info, user_id);
  return SqlResult(std::move(resultset));
}

// Execute a (select) statement against all shards.
SqlResult PeltonExecutor::All(const sqlast::AbstractStatement *sql,
                              const dataflow::SchemaRef &schema,
                              int aug_index) {
#ifndef PELTON_OPT
  LOG(INFO) << "All shards statement: " << sql;
#endif
  std::vector<AugInfo> aug_info;
  if (aug_index > -1) {
    aug_info.push_back({aug_index, ""});
  }
  SqlResultSet resultset =
      this->connection_->ExecuteQueryAll(sql, schema, aug_info);
  return SqlResult(std::move(resultset));
}

// Close any open singletons.
void PeltonExecutor::CloseAll() { RocksdbConnection::CloseAll(); }

}  // namespace sql
}  // namespace pelton
