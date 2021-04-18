// Manages mysql connections to the different shard/mini-databases.
#include "pelton/shards/pool.h"

#include <cstring>
#include <utility>

#include "glog/logging.h"
#include "mysql-cppconn-8/mysqlx/xdevapi.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/perf.h"

namespace pelton {
namespace shards {

#define SESSION reinterpret_cast<mysqlx::Session *>(this->session_)

// Destructor.
ConnectionPool::~ConnectionPool() {
  if (SESSION != nullptr) {
    SESSION->close();
    delete SESSION;
    this->session_ = nullptr;
  }
}

// Initialization: open a connection to the default unsharded database.
void ConnectionPool::Initialize(const std::string &username,
                                const std::string &password) {
  std::string connection_str = username + ":" + password + "@localhost";
  this->session_ = new mysqlx::Session(connection_str);
}

// Manage using shards in session_.
void ConnectionPool::OpenDefaultShard() {
  LOG(INFO) << "Shard: default shard";
  SESSION->sql("CREATE DATABASE IF NOT EXISTS default_db;").execute();
  SESSION->sql("USE default_db").execute();
}
void ConnectionPool::OpenShard(const ShardKind &shard_kind,
                               const UserId &user_id) {
  std::string shard_name = sqlengine::NameShard(shard_kind, user_id);
  LOG(INFO) << "Shard: " << shard_name;
  SESSION->sql("CREATE DATABASE IF NOT EXISTS " + shard_name).execute();
  SESSION->sql("USE " + shard_name).execute();
}

// Execution of SQL statements.
// Execute statement against the default un-sharded database.
SqlResult ConnectionPool::ExecuteDefault(const std::string &sql) {
  perf::Start("ExecuteDefault");

  this->OpenDefaultShard();
  LOG(INFO) << "Statement: " << sql;

  SqlResult result;
  result.AddResult(SESSION->sql(sql).execute());

  perf::End("ExecuteDefault");
  return result;
}

// Execute statement against given user shard(s).
SqlResult ConnectionPool::ExecuteShard(const std::string &sql,
                                       const ShardingInformation &info,
                                       const UserId &user_id) {
  perf::Start("ExecuteShard");

  this->OpenShard(info.shard_kind, user_id);
  LOG(INFO) << "Statement:" << sql;

  mysqlx::SqlResult result = SESSION->sql(sql).execute();
  if (result.hasData()) {
    SqlResult wrapper{info.shard_by_index,
                      Column{mysqlx::Type::STRING, info.shard_by}};
    wrapper.AddResult(std::move(result), mysqlx::Value(user_id));

    perf::End("ExecuteShard");
    return wrapper;
  } else {
    SqlResult wrapper;
    wrapper.AddResult(std::move(result));

    perf::End("ExecuteShard");
    return wrapper;
  }
}

SqlResult ConnectionPool::ExecuteShards(
    const std::string &sql, const ShardingInformation &info,
    const std::unordered_set<UserId> &user_ids) {
  // This result set is a proxy that allows access to results from all shards.
  SqlResult result;

  for (const UserId &user_id : user_ids) {
    SqlResult inner_result = this->ExecuteShard(sql, info, user_id);
    result.Consume(&inner_result);
  }

  return result;
}

// Removing a shard is equivalent to deleting its database.
void ConnectionPool::RemoveShard(const std::string &shard_name) {
  LOG(INFO) << "Remove Shard: " << shard_name;
  SESSION->sql("DROP DATABASE " + shard_name).execute();
}

}  // namespace shards
}  // namespace pelton
