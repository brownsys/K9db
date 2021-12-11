// Intermediate layer between pelton and whatever underlying DB we use.
#include "pelton/sql/executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/sql/connections/rocksdb_connection.h"
#include "pelton/util/perf.h"

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
  return this->Execute(sql, schema, __UNSHARDED_DB, __NO_AUG, __NO_AUG_VALUE);
}

// Execute statement against given user shard.
SqlResult PeltonExecutor::Shard(const sqlast::AbstractStatement *sql,
                                const std::string &shard_kind,
                                const shards::UserId &user_id,
                                const dataflow::SchemaRef &schema,
                                int aug_index) {
  // Find the physical shard name (prefix) by hashing the user id and user kind.
  perf::Start("hashing");
  std::string shard_name = shards::sqlengine::NameShard(shard_kind, user_id);
  perf::End("hashing");

#ifndef PELTON_OPT
  LOG(INFO) << "Shard: " << shard_name << " (userid: " << user_id << ")";
#endif

  // Execute against the underlying shard.
  return this->Execute(sql, schema, shard_name, aug_index, user_id);
}

// Execute statement against given user shards.
SqlResult PeltonExecutor::Shards(
    const sqlast::AbstractStatement *sql, const std::string &shard_kind,
    const std::unordered_set<shards::UserId> &user_ids,
    const dataflow::SchemaRef &schema, int aug_index) {
  // If no user_ids are provided, we return an empty result.
  if (user_ids.size() == 0) {
    return EmptyResult(sql, schema);
  }

  // This result set is a proxy that allows access to results from all shards.
  SqlResult result(schema);
  for (const shards::UserId &user_id : user_ids) {
    result.Append(this->Shard(sql, shard_kind, user_id, schema, aug_index),
                  true);
  }

  return result;
}

// Actual SQL statement execution.
SqlResult PeltonExecutor::Execute(const sqlast::AbstractStatement *stmt,
                                  const dataflow::SchemaRef &schema,
                                  const std::string &shard_name, int aug_index,
                                  const std::string &aug_value) {
#ifndef PELTON_OPT
  std::string sql = sqlast::Stringifier(shard_name).Visit(stmt);
  LOG(INFO) << "Statement: " << sql;
#endif

  switch (stmt->type()) {
    // Statements: return a boolean signifying success.
    case sqlast::AbstractStatement::Type::CREATE_TABLE:
    case sqlast::AbstractStatement::Type::CREATE_INDEX:
      return SqlResult(this->connection_->ExecuteStatement(stmt, shard_name));

    // Updates: return a count of rows affected.
    case sqlast::AbstractStatement::Type::UPDATE:
      return SqlResult(this->connection_->ExecuteUpdate(stmt, shard_name));

    // Insert and Delete might be returning.
    case sqlast::AbstractStatement::Type::INSERT:
      if (!static_cast<const sqlast::Insert *>(stmt)->returning()) {
        return SqlResult(this->connection_->ExecuteUpdate(stmt, shard_name));
      }
      break;
    case sqlast::AbstractStatement::Type::DELETE:
      if (!static_cast<const sqlast::Delete *>(stmt)->returning()) {
        return SqlResult(this->connection_->ExecuteUpdate(stmt, shard_name));
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
  if (aug_index > __NO_AUG) {
    aug_info.push_back({static_cast<size_t>(aug_index), aug_value});
  }
  // Create a result set for output.
  SqlResultSet resultset =
      this->connection_->ExecuteQuery(stmt, schema, aug_info, shard_name);
  return SqlResult(std::move(resultset));
}

// Close any open singletons.
void PeltonExecutor::CloseAll() { RocksdbConnection::CloseAll(); }

}  // namespace sql
}  // namespace pelton
