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
    Operation op, const std::string &sql, const dataflow::SchemaRef &schema) {
  perf::Start("ExecuteDefault");

  LOG(INFO) << "Shard: default shard";
  LOG(INFO) << "Statement: " << sql;

  mysql::SqlResult result = this->ExecuteMySQL(op, sql, schema);

  perf::End("ExecuteDefault");
  return result;
}

// Execute statement against given user shard(s).
mysql::SqlResult ConnectionPool::ExecuteShard(
    Operation op, const std::string &sql, const ShardingInformation &info,
    const UserId &user_id, const dataflow::SchemaRef &schema) {
  perf::Start("ExecuteShard");

  std::string shard_name = sqlengine::NameShard(info.shard_kind, user_id);
  LOG(INFO) << "Shard: " << shard_name << " (userid: " << user_id << ")";
  LOG(INFO) << "Statement:" << sql;

  mysql::SqlResult result = this->ExecuteMySQL(op, sql, schema, shard_name,
                                               info.shard_by_index, user_id);

  perf::End("ExecuteShard");
  return result;
}

mysql::SqlResult ConnectionPool::ExecuteShards(
    Operation op, const std::string &sql, const ShardingInformation &info,
    const std::unordered_set<UserId> &user_ids,
    const dataflow::SchemaRef &schema) {
  if (user_ids.size() == 0) {
    switch (op) {
      case ConnectionPool::Operation::STATEMENT:
        return mysql::SqlResult{std::make_unique<mysql::StatementResult>(true),
                                {}};
      case ConnectionPool::Operation::UPDATE:
        return mysql::SqlResult{std::make_unique<mysql::UpdateResult>(0), {}};
      case ConnectionPool::Operation::QUERY:
        return mysql::SqlResult{std::make_unique<mysql::InlinedSqlResult>(),
                                schema};
    }
  }

  // This result set is a proxy that allows access to results from all shards.
  mysql::SqlResult result;
  for (const UserId &user_id : user_ids) {
    result.MakeInline();
    result.Append(this->ExecuteShard(op, sql, info, user_id, schema));
  }

  return result;
}

// Removing a shard is equivalent to deleting its database.
void ConnectionPool::RemoveShard(const std::string &shard_name) {
  LOG(INFO) << "Remove Shard: " << shard_name;
  this->ExecuteMySQL(ConnectionPool::Operation::STATEMENT,
                     "DROP DATABASE " + shard_name, {});
}

// Actual SQL statement execution.
mysql::SqlResult ConnectionPool::ExecuteMySQL(ConnectionPool::Operation op,
                                              const std::string &sql,
                                              const dataflow::SchemaRef &schema,
                                              const std::string &shard_name,
                                              int aug_index,
                                              const std::string &aug_value) {
  if (this->databases_.count(shard_name) == 0) {
    this->stmt_->execute("CREATE DATABASE IF NOT EXISTS " + shard_name);
    this->databases_.insert(shard_name);
  }

  std::string exec_str = "USE " + shard_name + "; " + sql;
  mysql::SqlResult res;
  switch (op) {
    case ConnectionPool::Operation::STATEMENT: {
      perf::Start("MySQL");
      this->stmt_->execute(exec_str);
      this->stmt_->getMoreResults();
      this->stmt_->getMoreResults();
      perf::End("MySQL");
      res = mysql::SqlResult(std::make_unique<mysql::StatementResult>(true));
      break;
    }
    case ConnectionPool::Operation::UPDATE: {
      perf::Start("MySQL");
      this->stmt_->execute(exec_str);
      this->stmt_->getMoreResults();  // Skip all results.
      int row_count = this->stmt_->getUpdateCount();
      this->stmt_->getMoreResults();
      perf::End("MySQL");
      res = mysql::SqlResult(std::make_unique<mysql::UpdateResult>(row_count));
      break;
    }
    case ConnectionPool::Operation::QUERY: {
      perf::Start("MySQL");
      sql::ResultSet *tmp;
      this->stmt_->execute(exec_str);
      this->stmt_->getMoreResults();  // Skip the USE <db>; result.
      tmp = this->stmt_->getResultSet();
      this->stmt_->getMoreResults();
      perf::End("MySQL");
      if (aug_index > -1) {
        res = mysql::SqlResult(
            std::make_unique<mysql::AugmentedSqlResult>(
                std::unique_ptr<sql::ResultSet>(tmp), aug_index, aug_value),
            schema);
      } else {
        res = mysql::SqlResult(std::make_unique<mysql::MySqlResult>(
                                   std::unique_ptr<sql::ResultSet>(tmp)),
                               schema);
      }
      break;
    }
  }

  return res;
}

}  // namespace shards
}  // namespace pelton
