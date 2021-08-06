// Manages mysql connections to the different shard/mini-databases.
#include "pelton/sql/lazy_executor.h"

#include <memory>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/sql/result/resultset.h"
#include "pelton/util/perf.h"

namespace pelton {
namespace sql {

// Creates an empty SqlResult that corresponds to the given sql statement type.
SqlResult SqlLazyExecutor::EmptyResultByType(
    sqlast::AbstractStatement::Type type, bool returning,
    const dataflow::SchemaRef &schema) {
  switch (type) {
    case sqlast::AbstractStatement::Type::CREATE_TABLE:
    case sqlast::AbstractStatement::Type::CREATE_INDEX:
      return SqlResult(true);
    case sqlast::AbstractStatement::Type::INSERT:
    case sqlast::AbstractStatement::Type::UPDATE:
      return SqlResult(0);
    case sqlast::AbstractStatement::Type::DELETE:
      if (!returning) {
        return SqlResult(0);
      }
    case sqlast::AbstractStatement::Type::SELECT:
      return SqlResult(std::make_unique<_result::SqlLazyResultSet>(
          schema, &this->eager_executor_));
    default:
      LOG(FATAL) << "Unsupported SQL statement in SqlLazyExecutor";
  }
}

// Initialization: initialize the eager executor so that it maintains
// an open connection to the underlying DB.
void SqlLazyExecutor::Initialize(const std::string &db_name,
                                 const std::string &username,
                                 const std::string &password) {
  this->eager_executor_.Initialize(db_name, username, password);
}

// (lazy) Execution of SQL statements.
// Execute statement against the default un-sharded database.
SqlResult SqlLazyExecutor::ExecuteDefault(const sqlast::AbstractStatement *sql,
                                          const dataflow::SchemaRef &schema) {
#ifndef PELTON_OPT
  LOG(INFO) << "Shard: default";
#endif
  return this->Execute(sql, schema);
}

// Execute statement against given user shard.
SqlResult SqlLazyExecutor::ExecuteShard(const sqlast::AbstractStatement *sql,
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
SqlResult SqlLazyExecutor::ExecuteShards(
    const sqlast::AbstractStatement *sql, const std::string &shard_kind,
    const std::unordered_set<shards::UserId> &user_ids,
    const dataflow::SchemaRef &schema, int aug_index) {
  // If no user_ids are provided, we return an empty result.
  if (user_ids.size() == 0) {
    bool returning = false;
    if (sql->type() == sqlast::AbstractStatement::Type::DELETE) {
      returning = static_cast<const sqlast::Delete *>(sql)->returning();
    }
    return EmptyResultByType(sql->type(), returning, schema);
  }

  // This result set is a proxy that allows access to results from all shards.
  SqlResult result;
  for (const shards::UserId &user_id : user_ids) {
    result.Append(
        this->ExecuteShard(sql, shard_kind, user_id, schema, aug_index));
  }

  return result;
}

// Actual SQL statement execution.
SqlResult SqlLazyExecutor::Execute(const sqlast::AbstractStatement *stmt,
                                   const dataflow::SchemaRef &schema,
                                   const std::string &shard_name, int aug_index,
                                   const std::string &aug_value) {
  std::string sql = sqlast::Stringifier(shard_name).Visit(stmt);
#ifndef PELTON_OPT
  LOG(INFO) << "Statement: " << sql;
#endif

  switch (stmt->type()) {
    // Statements: return a boolean signifying success.
    case sqlast::AbstractStatement::Type::CREATE_TABLE:
    case sqlast::AbstractStatement::Type::CREATE_INDEX:
      return SqlResult(this->eager_executor_.ExecuteStatement(sql));

    // Updates: return a count of rows affected.
    case sqlast::AbstractStatement::Type::INSERT:
    case sqlast::AbstractStatement::Type::UPDATE:
      return SqlResult(this->eager_executor_.ExecuteUpdate(sql));

    // A returning delete is a query, a non returning delete is an update.
    case sqlast::AbstractStatement::Type::DELETE:
      if (!static_cast<const sqlast::Delete *>(stmt)->returning()) {
        return SqlResult(this->eager_executor_.ExecuteUpdate(sql));
      }
      // No break, next case is executed.

    // Queries: return a (lazy) result set of records.
    case sqlast::AbstractStatement::Type::SELECT: {
      // Set up augmentation information to augment any sharding columns to the
      // result set.
      std::vector<_result::AugmentingInformation> aug_info;
      if (aug_index > -1) {
        aug_info.push_back({static_cast<size_t>(aug_index), aug_value});
      }
      // Create a lazy result set.
      std::unique_ptr<_result::SqlLazyResultSet> result_set =
          std::make_unique<_result::SqlLazyResultSet>(schema,
                                                      &this->eager_executor_);
      result_set->AddShardResult({sql, std::move(aug_info)});
      return SqlResult(std::move(result_set));
    }

    default:
      LOG(FATAL) << "Unsupported SQL statement in pool.cc";
  }
}

}  // namespace sql
}  // namespace pelton
