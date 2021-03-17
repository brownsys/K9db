// Manages sqlite3 connections to the different shard/mini-databases.
#ifndef PELTON_SHARDS_SQLEXECUTOR_POOL_H_
#define PELTON_SHARDS_SQLEXECUTOR_POOL_H_

#include <sqlite3.h>

#include <string>

namespace pelton {
namespace shards {
namespace sqlexecutor {

class ConnectionPool {
 public:
  // Constructor.
  ConnectionPool() = default;

  // Not copyable or movable.
  ConnectionPool(const ConnectionPool &) = delete;
  ConnectionPool &operator=(const ConnectionPool &) = delete;
  ConnectionPool(const ConnectionPool &&) = delete;
  ConnectionPool &operator=(const ConnectionPool &&) = delete;

  // Destructor.
  ~ConnectionPool();

  void Initialize(const std::string &dir_path);

  ::sqlite3 *GetDefaultConnection() const {
    return this->default_noshard_connection_;
  }

  ::sqlite3 *GetConnection(const std::string &shard_kind,
                           const std::string &user_id) const;

 private:
  std::string dir_path_;
  // We always keep an open connection to the main non-sharded database.
  ::sqlite3 *default_noshard_connection_;
};

}  // namespace sqlexecutor
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLEXECUTOR_POOL_H_
