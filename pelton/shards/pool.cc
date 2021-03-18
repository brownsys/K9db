// Manages sqlite3 connections to the different shard/mini-databases.
#include "pelton/shards/pool.h"

#include "absl/strings/str_cat.h"
#include "glog/logging.h"
#include "pelton/shards/sqlengine/util.h"

namespace pelton {
namespace shards {

// Destructor: close connection to the default unsharded database.
ConnectionPool::~ConnectionPool() {
  if (this->default_noshard_connection_ != nullptr) {
    ::sqlite3_close(this->default_noshard_connection_);
  }
  this->default_noshard_connection_ = nullptr;
}

// Initialization: open a connection to the default unsharded database.
void ConnectionPool::Initialize(const std::string &dir_path) {
  this->dir_path_ = dir_path;
  std::string shard_path = dir_path + std::string("default.sqlite3");
  ::sqlite3_open(shard_path.c_str(), &this->default_noshard_connection_);
}

// Get conenction to default shard (always open).
::sqlite3 *ConnectionPool::GetDefaultConnection() const {
  LOG(INFO) << "Shard: default shard";
  return this->default_noshard_connection_;
}

// Open a connection to a shard.
::sqlite3 *ConnectionPool::GetConnection(const ShardKind &shard_kind,
                                         const UserId &user_id) const {
  // Find the shard path.
  std::string shard_name = sqlengine::NameShard(shard_kind, user_id);
  std::string shard_path = this->dir_path_ + shard_name;
  // Open and return connection.
  LOG(INFO) << "Shard: " << shard_name;
  ::sqlite3 *connection;
  ::sqlite3_open(shard_path.c_str(), &connection);
  return connection;
}

// Execution of SQL statements.
// Execute statement against the default un-sharded database.
absl::Status ConnectionPool::ExecuteDefault(const std::string &sql,
                                            const OutputChannel &output) {
  ::sqlite3 *connection = this->GetDefaultConnection();
  return this->Execute(sql, connection, output);
}

// Execute statement against given user shard(s).
absl::Status ConnectionPool::ExecuteShard(const std::string &sql,
                                          const ShardKind &shard_kind,
                                          const UserId &user_id,
                                          const OutputChannel &output) {
  ::sqlite3 *connection = this->GetConnection(shard_kind, user_id);
  return this->Execute(sql, connection, output);
}

absl::Status ConnectionPool::ExecuteShard(
    const std::string &sql, const ShardKind &shard_kind,
    const std::unordered_set<UserId> &user_ids, const OutputChannel &output) {
  for (const UserId &user_id : user_ids) {
    ::sqlite3 *connection = this->GetConnection(shard_kind, user_id);
    absl::Status result = this->Execute(sql, connection, output);
    if (!result.ok()) {
      return result;
    }
  }
  return absl::OkStatus();
}

// Actually execute statements after their connection has been resolved.
absl::Status ConnectionPool::Execute(const std::string &sql,
                                     ::sqlite3 *connection,
                                     const OutputChannel &output) {
  LOG(INFO) << "Statement: " << sql;
  // Turn c++ style callback into a c-style function pointer.
  static const Callback *CALLBACK_NO_CAPTURE = &output.callback;
  auto cb = [](void *context, int colnum, char **colvals, char **colnames) {
    return (*CALLBACK_NO_CAPTURE)(context, colnum, colvals, colnames);
  };

  // Execute using c-style function pointer.
  const char *str = sql.c_str();
  void *context = output.context;
  char **errmsg = output.errmsg;
  if (::sqlite3_exec(connection, str, cb, context, errmsg) == SQLITE_OK) {
    return absl::OkStatus();
  } else {
    std::string error = "Sqlite3 statement failed";
    if (*errmsg != nullptr) {
      error = absl::StrCat(error, ": ", *errmsg);
    }
    return absl::AbortedError(error);
  }
}

}  // namespace shards
}  // namespace pelton
