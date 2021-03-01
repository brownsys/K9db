// Manages sqlite3 connections to the different shard/mini-databases.

#include "pelton/shards/sqlexecutor/executor.h"

#include <cstring>
#include <vector>

namespace pelton {
namespace shards {
namespace sqlexecutor {

namespace {

std::string Concatenate(int colnum, char **colvals) {
  std::string concatenate = "";
  for (int i = 0; i < colnum; i++) {
    concatenate += colvals[i];
  }
  return concatenate;
}

}  // namespace

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

void SQLExecutor::StartBlock() { this->deduplication_buffer_.clear(); }

// Executing a statement.
bool SQLExecutor::ExecuteStatement(
    std::unique_ptr<ExecutableStatement> statement, const Callback &callback,
    void *context, char **errmsg) {
  // Open connection (if needed).
  ::sqlite3 *connection = this->default_noshard_connection_;
  const std::string &shard_suffix = statement->shard_suffix();
  if (shard_suffix != "default") {
    std::string shard_path =
        this->dir_path_ + shard_suffix + std::string(".sqlite3");
    ::sqlite3_open(shard_path.c_str(), &connection);
  }

  // Execute query.
  bool result = statement->Execute(
      connection,
      [&](void *context, int colnum, char **colvals, char **colnames) {
        std::string row = Concatenate(colnum, colvals);
        if (this->deduplication_buffer_.count(row) == 0) {
          this->deduplication_buffer_.insert(row);
          return callback(context, colnum, colvals, colnames);
        }
        return 0;
      },
      context, errmsg);

  // Close connection (if needed).
  if (shard_suffix != "default") {
    ::sqlite3_close(connection);
  }

  return result;
}

}  // namespace sqlexecutor
}  // namespace shards
}  // namespace pelton
