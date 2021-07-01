// Manages mysql connections to the different shard/mini-databases.
#include "pelton/sql/executor.h"

#include <cstring>
#include <utility>

#include "glog/logging.h"
#include "mariadb/conncpp.hpp"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/perf.h"

namespace pelton {
namespace sql {

// MariaDB API types.
using DBProperties = ::sql::ConnectOptionsMap;
using DBDriver = ::sql::Driver;
using DBConnection = ::sql::Connection;
using DBStatement = ::sql::Statement;
using DBResultSet = ::sql::ResultSet;

// Initialization: open a connection to the default unsharded database.
void SqlExecutor::Initialize(const std::string &username,
                             const std::string &password) {
  DBProperties props;
  props["hostName"] = "localhost";
  props["userName"] = username;
  props["password"] = password;
  // props["CLIENT_MULTI_STATEMENTS"] = true;

  DBDriver *driver = ::sql::mariadb::get_driver_instance();
  this->conn_ = std::unique_ptr<DBConnection>(driver->connect(props));
  this->stmt_ = std::unique_ptr<DBStatement>(this->conn_->createStatement());

  // Create and use the DB.
  this->stmt_->execute("CREATE DATABASE IF NOT EXISTS pelton");
  this->stmt_->execute("USE pelton");
  // this->stmt_->execute("SET GLOBAL table_open_cache=50000");
  // this->stmt_->execute("SET GLOBAL schema_definition_cache=10000");
  // this->stmt_->execute("SET GLOBAL table_definition_cache=10000");
}

// Execution of SQL statements.
// Execute statement against the default un-sharded database.
SqlResult SqlExecutor::ExecuteDefault(const sqlast::AbstractStatement *sql,
                                      const dataflow::SchemaRef &schema) {
#ifndef PELTON_OPT
  LOG(INFO) << "Shard: default";
#endif
  return this->ExecuteMySQL(sql, schema);
}

// Execute statement against given user shard(s).
SqlResult SqlExecutor::ExecuteShard(const sqlast::AbstractStatement *sql,
                                    const shards::ShardingInformation &info,
                                    const shards::UserId &user_id,
                                    const dataflow::SchemaRef &schema) {
  perf::Start("hashing");
  std::string shard_name =
      shards::sqlengine::NameShard(info.shard_kind, user_id);
  perf::End("hashing");
#ifndef PELTON_OPT
  LOG(INFO) << "Shard: " << shard_name << " (userid: " << user_id << ")";
#endif
  if (info.IsTransitive()) {
    return this->ExecuteMySQL(sql, schema, shard_name);
  } else {
    return this->ExecuteMySQL(sql, schema, shard_name, info.shard_by_index,
                              user_id);
  }
}

SqlResult SqlExecutor::ExecuteShards(
    const sqlast::AbstractStatement *sql,
    const shards::ShardingInformation &info,
    const std::unordered_set<shards::UserId> &user_ids,
    const dataflow::SchemaRef &schema) {
  if (user_ids.size() == 0) {
    switch (sql->type()) {
      case sqlast::AbstractStatement::Type::CREATE_TABLE:
      case sqlast::AbstractStatement::Type::CREATE_INDEX:
        return SqlResult{std::make_unique<StatementResult>(true), {}};
      case sqlast::AbstractStatement::Type::INSERT:
      case sqlast::AbstractStatement::Type::UPDATE:
        return SqlResult{std::make_unique<UpdateResult>(0), {}};
      case sqlast::AbstractStatement::Type::DELETE:
        if (!static_cast<const sqlast::Delete *>(sql)->returning()) {
          return SqlResult{std::make_unique<UpdateResult>(0), {}};
        }
      case sqlast::AbstractStatement::Type::SELECT:
        return SqlResult{std::make_unique<InlinedSqlResult>(), schema};
      default:
        LOG(FATAL) << "Unsupported SQL statement in pool.cc";
    }
  }

  // This result set is a proxy that allows access to results from all shards.
  SqlResult result;
  for (const shards::UserId &user_id : user_ids) {
    result.MakeInline();
    result.Append(this->ExecuteShard(sql, info, user_id, schema));
  }

  return result;
}

// Actual SQL statement execution.
SqlResult SqlExecutor::ExecuteMySQL(const sqlast::AbstractStatement *stmt,
                                    const dataflow::SchemaRef &schema,
                                    const std::string &shard_name,
                                    int aug_index,
                                    const std::string &aug_value) {
  std::string sql = sqlast::Stringifier(shard_name).Visit(stmt);
#ifndef PELTON_OPT
  LOG(INFO) << "Statement:" << sql;
#endif

  switch (stmt->type()) {
    case sqlast::AbstractStatement::Type::CREATE_TABLE:
    case sqlast::AbstractStatement::Type::CREATE_INDEX: {
      perf::Start("MySQL");
      this->stmt_->execute(sql);
      perf::End("MySQL");
      return SqlResult(std::make_unique<StatementResult>(true));
    }

    case sqlast::AbstractStatement::Type::UPDATE:
    case sqlast::AbstractStatement::Type::INSERT: {
      perf::Start("MySQL");
      int count = this->stmt_->executeUpdate(sql);
      perf::End("MySQL");
      return SqlResult(std::make_unique<UpdateResult>(count));
    }
    // A returning delete is a query, a non returning delete is an update.
    case sqlast::AbstractStatement::Type::DELETE: {
      if (!static_cast<const sqlast::Delete *>(stmt)->returning()) {
        perf::Start("MySQL");
        int count = this->stmt_->executeUpdate(sql);
        perf::End("MySQL");
        return SqlResult(std::make_unique<UpdateResult>(count));
      }
      // No break, next case is executed.
    }
    case sqlast::AbstractStatement::Type::SELECT: {
      perf::Start("MySQL");
      std::unique_ptr<DBResultSet> tmp{this->stmt_->executeQuery(sql)};
      perf::End("MySQL");
      if (aug_index > -1) {
        return SqlResult(std::make_unique<AugmentedSqlResult>(
                             std::move(tmp), aug_index, aug_value),
                         schema);
      } else {
        return SqlResult(std::make_unique<MySqlResult>(std::move(tmp)), schema);
      }
    }
    default:
      LOG(FATAL) << "Unsupported SQL statement in pool.cc";
  }
}

}  // namespace sql
}  // namespace pelton
