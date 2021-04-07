// Manages sqlite3 connections to the different shard/mini-databases.
#include "pelton/shards/pool.h"

#include <cstring>

#include "absl/strings/str_cat.h"
#include "glog/logging.h"
#include "pelton/shards/sqlengine/util.h"

namespace pelton {
namespace shards {

namespace {

char *CopyCString(const std::string &str) {
  char *result = new char[str.size() + 1];
  // NOLINTNEXTLINE
  strcpy(result, str.c_str());
  return result;
}

}  // namespace

const Callback *ConnectionPool::CALLBACK_NO_CAPTURE = nullptr;
size_t ConnectionPool::SHARD_BY_INDEX_NO_CAPTURE = 0;
char *ConnectionPool::SHARD_BY_NO_CAPTURE = nullptr;
char *ConnectionPool::USER_ID_NO_CAPTURE = nullptr;

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
  std::string shard_path = absl::StrCat(dir_path, "default.sqlite3");
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
  std::string shard_path = absl::StrCat(this->dir_path_, shard_name);
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
  return this->ExecuteDefault(sql, connection, output);
}

// Execute statement against given user shard(s).
absl::Status ConnectionPool::ExecuteShard(const std::string &sql,
                                          const ShardingInformation &info,
                                          const UserId &user_id,
                                          const OutputChannel &output) {
  ::sqlite3 *connection = this->GetConnection(info.shard_kind, user_id);
  return this->ExecuteShard(sql, connection, info, user_id, output);
}

absl::Status ConnectionPool::ExecuteShard(
    const std::string &sql, const ShardingInformation &info,
    const std::unordered_set<UserId> &user_ids, const OutputChannel &output) {
  for (const UserId &user_id : user_ids) {
    ::sqlite3 *connection = this->GetConnection(info.shard_kind, user_id);
    auto result = this->ExecuteShard(sql, connection, info, user_id, output);
    if (!result.ok()) {
      return result;
    }
  }
  return absl::OkStatus();
}

// Actually execute statements after their connection has been resolved.
absl::Status ConnectionPool::ExecuteDefault(const std::string &sql,
                                            ::sqlite3 *connection,
                                            const OutputChannel &output) {
  LOG(INFO) << "Statement: " << sql;
  // Turn c++ style callback into a c-style function pointer.
  CALLBACK_NO_CAPTURE = &output.callback;
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

absl::Status ConnectionPool::ExecuteShard(const std::string &sql,
                                          ::sqlite3 *connection,
                                          const ShardingInformation &info,
                                          const UserId &user_id,
                                          const OutputChannel &output) {
  LOG(INFO) << "Statement: " << sql;
  // Turn c++ style callback into a c-style function pointer.
  CALLBACK_NO_CAPTURE = &output.callback;
  SHARD_BY_INDEX_NO_CAPTURE = info.shard_by_index;
  SHARD_BY_NO_CAPTURE = CopyCString(info.shard_by);
  USER_ID_NO_CAPTURE = CopyCString(user_id);
  // Augment the shard by column to the result.
  auto cb = [](void *context, int _colnum, char **colvals, char **colnames) {
    size_t colnum = static_cast<size_t>(_colnum);
    // Append stored column name and value to result at the stored index.
    char **vals = new char *[colnum + 1];
    char **names = new char *[colnum + 1];
    vals[SHARD_BY_INDEX_NO_CAPTURE] = USER_ID_NO_CAPTURE;
    names[SHARD_BY_INDEX_NO_CAPTURE] = SHARD_BY_NO_CAPTURE;
    for (size_t i = 0; i < SHARD_BY_INDEX_NO_CAPTURE; i++) {
      vals[i] = colvals[i];
      names[i] = colnames[i];
    }
    for (size_t i = SHARD_BY_INDEX_NO_CAPTURE; i < colnum; i++) {
      vals[i + 1] = colvals[i];
      names[i + 1] = colnames[i];
    }
    // Forward appended results to callback.
    int result = (*CALLBACK_NO_CAPTURE)(context, colnum + 1, vals, names);
    // Deallocate appended results.
    delete[] vals;
    delete[] names;
    return result;
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
