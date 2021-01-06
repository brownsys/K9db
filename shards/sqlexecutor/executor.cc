// Manages sqlite3 connections to the different shard/mini-databases.

#include "shards/sqlexecutor/executor.h"

#include <cstring>
#include <vector>

namespace shards {
namespace sqlexecutor {

// Initialization and destruction: manage the always open connection the main
// unsharded database.
void SQLExecutor::Initialize(const std::string &dir_path) {
  this->dir_path_ = dir_path;
  std::string shard_path = dir_path + std::string("default.sqlite3");
  ::sqlite3_open(shard_path.c_str(), &this->default_noshard_connection_);
}

SQLExecutor::~SQLExecutor() {
  ::sqlite3_close(this->default_noshard_connection_);
}

// Executing a statement.
bool SQLExecutor::ExecuteStatement(
    std::unique_ptr<ExecutableStatement> statement, Callback callback,
    void *context, char **errmsg) const {
  // Open connection (if needed).
  ::sqlite3 *connection = this->default_noshard_connection_;
  const std::string &shard_suffix = statement->shard_suffix();
  if (shard_suffix != "default") {
    std::string shard_path =
        this->dir_path_ + shard_suffix + std::string(".sqlite3");
    ::sqlite3_open(shard_path.c_str(), &connection);
  }

  // Execute query.
  bool result = statement->Execute(connection, callback, context, errmsg);

  // Close connection (if needed).
  if (shard_suffix != "default") {
    ::sqlite3_close(connection);
  }

  return result;
}

}  // namespace sqlexecutor
}  // namespace shards
