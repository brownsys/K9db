// Manages mysql connections to the different shard/mini-databases.
#include "pelton/shards/pool.h"

#include <cstring>
#include <utility>

#include "glog/logging.h"
#include "mysql-cppconn-8/jdbc/cppconn/resultset.h"
#include "mysql-cppconn-8/jdbc/mysql_driver.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/perf.h"

namespace pelton {
namespace shards {

// Initialization: open a connection to the default unsharded database.
void ConnectionPool::Initialize(const std::string &username,
                                const std::string &password) {
  sql::ConnectOptionsMap connection_properties;
  connection_properties["hostName"] = "localhost";
  connection_properties["userName"] = username;
  connection_properties["password"] = password;
  connection_properties["CLIENT_MULTI_STATEMENTS"] = true;

  sql::Driver *driver = sql::mysql::get_driver_instance();
  this->conn_ =
      std::unique_ptr<sql::Connection>(driver->connect(connection_properties));
  this->stmt_ = std::unique_ptr<sql::Statement>(this->conn_->createStatement());
}

// Execution of SQL statements.
// Execute statement against the default un-sharded database.
mysql::SqlResult ConnectionPool::ExecuteDefault(
    const sqlast::AbstractStatement *sql, const dataflow::SchemaRef &schema) {
  LOG(INFO) << "Shard: default";
  return this->ExecuteMySQL(sql, schema);
}

// Execute statement against given user shard(s).
mysql::SqlResult ConnectionPool::ExecuteShard(
    const sqlast::AbstractStatement *sql, const ShardingInformation &info,
    const UserId &user_id, const dataflow::SchemaRef &schema) {
  std::string shard_name = sqlengine::NameShard(info.shard_kind, user_id);
  LOG(INFO) << "Shard: " << shard_name << " (userid: " << user_id << ")";
  return this->ExecuteMySQL(sql, schema, shard_name, info.shard_by_index,
                            user_id);
}

mysql::SqlResult ConnectionPool::ExecuteShards(
    const sqlast::AbstractStatement *sql, const ShardingInformation &info,
    const std::unordered_set<UserId> &user_ids,
    const dataflow::SchemaRef &schema) {
  if (user_ids.size() == 0) {
    switch (sql->type()) {
      case sqlast::AbstractStatement::Type::CREATE_TABLE:
      case sqlast::AbstractStatement::Type::CREATE_INDEX:
        return mysql::SqlResult{std::make_unique<mysql::StatementResult>(true),
                                {}};
      case sqlast::AbstractStatement::Type::INSERT:
      case sqlast::AbstractStatement::Type::UPDATE:
      case sqlast::AbstractStatement::Type::DELETE:
        return mysql::SqlResult{std::make_unique<mysql::UpdateResult>(0), {}};
      case sqlast::AbstractStatement::Type::SELECT:
        return mysql::SqlResult{std::make_unique<mysql::InlinedSqlResult>(),
                                schema};
      default:
        LOG(FATAL) << "Unsupported SQL statement in pool.cc";
    }
  }

  // This result set is a proxy that allows access to results from all shards.
  mysql::SqlResult result;
  for (const UserId &user_id : user_ids) {
    result.MakeInline();
    result.Append(this->ExecuteShard(sql, info, user_id, schema));
  }

  return result;
}

// Removing a shard is equivalent to deleting its database.
void ConnectionPool::RemoveShard(const std::string &shard_name) {
  LOG(INFO) << "Remove Shard: " << shard_name;
  this->stmt_->execute("DROP DATABASE " + shard_name);
}

// Actual SQL statement execution.
mysql::SqlResult ConnectionPool::ExecuteMySQL(
    const sqlast::AbstractStatement *stmt, const dataflow::SchemaRef &schema,
    const std::string &shard_name, int aug_index,
    const std::string &aug_value) {
  // Create database if it does not exist.
  if (this->databases_.count(shard_name) == 0) {
    this->stmt_->execute("CREATE DATABASE " + shard_name);
    this->databases_.insert(shard_name);
  }

  std::string sql = sqlast::Stringifier(shard_name).Visit(stmt);
  LOG(INFO) << "Statement:" << sql;

  switch (stmt->type()) {
    case sqlast::AbstractStatement::Type::CREATE_TABLE:
    case sqlast::AbstractStatement::Type::CREATE_INDEX: {
      perf::Start("MySQL");
      if (this->shard_in_use_ == shard_name) {
        this->stmt_->execute(sql);
      } else {
        LOG(INFO) << "USE DB";
        this->shard_in_use_ = shard_name;
        this->stmt_->execute("USE " + shard_name + "; " + sql);
        this->stmt_->getMoreResults();
        this->stmt_->getMoreResults();
      }
      perf::End("MySQL");
      return mysql::SqlResult(std::make_unique<mysql::StatementResult>(true));
    }

    case sqlast::AbstractStatement::Type::INSERT:
    case sqlast::AbstractStatement::Type::UPDATE:
    case sqlast::AbstractStatement::Type::DELETE: {
      perf::Start("MySQL");
      int count = this->stmt_->executeUpdate(sql);
      perf::End("MySQL");
      return mysql::SqlResult(std::make_unique<mysql::UpdateResult>(count));
    }

    case sqlast::AbstractStatement::Type::SELECT: {
      perf::Start("MySQL");
      std::unique_ptr<sql::ResultSet> tmp{this->stmt_->executeQuery(sql)};
      perf::End("MySQL");
      if (aug_index > -1) {
        return mysql::SqlResult(std::make_unique<mysql::AugmentedSqlResult>(
                                    std::move(tmp), aug_index, aug_value),
                                schema);
      } else {
        return mysql::SqlResult(
            std::make_unique<mysql::MySqlResult>(std::move(tmp)), schema);
      }
    }
    default:
      LOG(FATAL) << "Unsupported SQL statement in pool.cc";
  }
}

}  // namespace shards
}  // namespace pelton
