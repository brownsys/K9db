// Manages sqlite3 connections to the different shard/mini-databases.
#include "pelton/shards/sqlexecutor/pool.h"

#include <cstring>
#include <vector>

#include "pelton/shards/sqlengine/util.h"

namespace pelton {
namespace shards {
namespace sqlexecutor {

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

// Open a connection to a shard.
::sqlite3 *ConnectionPool::GetConnection(const std::string &shard_kind,
                                         const std::string &user_id) const {
  // Find the shard path.
  std::string shard_name = sqlengine::NameShard(shard_kind, user_id);
  std::string shard_path = this->dir_path_ + shard_name;
  // Open and return connection.
  ::sqlite3 *connection;
  ::sqlite3_open(shard_path.c_str(), &connection);
  return connection;
}

}  // namespace sqlexecutor
}  // namespace shards
}  // namespace pelton
