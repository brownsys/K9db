// Manages sqlite3 connections to the different shard/mini-databases.

#include "shards/sqlconnections/pool.h"

namespace shards {
namespace sqlconnections {

// Destructor
ConnectionPool::~ConnectionPool() { ::sqlite3_close(this->main_connection_); }

// Initialization.
void ConnectionPool::Initialize(const std::string &dir_path) {
  this->dir_path_ = dir_path;
  std::string shard_path = dir_path + std::string("default.sqlite3");
  ::sqlite3_open(shard_path.c_str(), &this->main_connection_);
}

bool ConnectionPool::ExecuteStatement(const std::string &shard_suffix,
                                      const std::string &statement) {
  // Open connection (if needed).
  sqlite3 *connection = this->main_connection_;
  if (shard_suffix != "default") {
    std::string shard_path =
        this->dir_path_ + shard_suffix + std::string(".sqlite3");
    ::sqlite3_open(shard_path.c_str(), &connection);
  }
  // Execute query.
  char *err_msg = nullptr;
  int result_code =
      ::sqlite3_exec(connection, statement.c_str(),
                     ConnectionPool::default_callback, 0, &err_msg);
  // Handle errors.
  if (result_code != SQLITE_OK) {
    throw "SQLITE3 exeuction error!";
  }
  // Close connection (if needed).
  if (shard_suffix != "default") {
    ::sqlite3_close(connection);
  }

  return result_code == SQLITE_OK;
}

}  // namespace sqlconnections
}  // namespace shards
