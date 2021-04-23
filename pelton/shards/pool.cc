// Manages mysql connections to the different shard/mini-databases.
#include "pelton/shards/pool.h"

#include <cstring>
#include <utility>

#include "glog/logging.h"
#include "mysql-cppconn-8/jdbc/cppconn/resultset.h"
#include "mysql-cppconn-8/jdbc/cppconn/statement.h"
#include "mysql-cppconn-8/jdbc/mysql_driver.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/perf.h"

namespace pelton {
namespace shards {

namespace {

mysql::SqlResult ExecuteMySQL(sql::Connection *conn,
                              ConnectionPool::Operation op,
                              const std::string &sql,
                              const dataflow::SchemaRef &schema,
                              const std::string &shard_name = "default_db",
                              int aug_index = -1,
                              const std::string &aug_value = "") {
  perf::Start("MySQL");
  std::unique_ptr<sql::Statement> stmt =
      std::unique_ptr<sql::Statement>(conn->createStatement());
  stmt->execute("CREATE DATABASE IF NOT EXISTS " + shard_name);
  stmt->execute("USE " + shard_name);

  mysql::SqlResult res;
  switch (op) {
    case ConnectionPool::Operation::STATEMENT: {
      stmt->execute(sql);
      res = mysql::SqlResult(std::make_unique<mysql::StatementResult>(true));
      break;
    }
    case ConnectionPool::Operation::UPDATE: {
      int row_count = stmt->executeUpdate(sql);
      res = mysql::SqlResult(std::make_unique<mysql::UpdateResult>(row_count));
      break;
    }
    case ConnectionPool::Operation::QUERY: {
      sql::ResultSet *tmp = stmt->executeQuery(sql);
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
  perf::End("MySQL");
  return res;
}

}  // namespace

// Initialization: open a connection to the default unsharded database.
void ConnectionPool::Initialize(const std::string &username,
                                const std::string &password) {
  sql::Driver *driver = sql::mysql::get_driver_instance();
  this->conn_ = std::unique_ptr<sql::Connection>(
      driver->connect("localhost", username, password));
}

// Execution of SQL statements.
// Execute statement against the default un-sharded database.
mysql::SqlResult ConnectionPool::ExecuteDefault(
    Operation op, const std::string &sql, const dataflow::SchemaRef &schema) {
  perf::Start("ExecuteDefault");

  LOG(INFO) << "Shard: default shard";
  LOG(INFO) << "Statement: " << sql;

  mysql::SqlResult result = ExecuteMySQL(this->conn_.get(), op, sql, schema);

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

  mysql::SqlResult result =
      ExecuteMySQL(this->conn_.get(), op, sql, schema, shard_name,
                   info.shard_by_index, user_id);

  perf::End("ExecuteShard");
  return result;
}

mysql::SqlResult ConnectionPool::ExecuteShards(
    Operation op, const std::string &sql, const ShardingInformation &info,
    const std::unordered_set<UserId> &user_ids,
    const dataflow::SchemaRef &schema) {
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
  ExecuteMySQL(this->conn_.get(), ConnectionPool::Operation::STATEMENT,
               "DROP DATABASE " + shard_name, {});
}

}  // namespace shards
}  // namespace pelton
