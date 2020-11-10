// Manages sqlite3 connections to the different shard/mini-databases.

#ifndef SHARDS_SQLCONNECTIONS_POOL_
#define SHARDS_SQLCONNECTIONS_POOL_

#include <sqlite3.h>

#include <string>

namespace shards {
namespace sqlconnections {

class ConnectionPool {
 public:
  // Constructor.
  ConnectionPool() {}

  // Not copyable or movable.
  ConnectionPool(const ConnectionPool &) = delete;
  ConnectionPool &operator=(const ConnectionPool &) = delete;
  ConnectionPool(const ConnectionPool &&) = delete;
  ConnectionPool &operator=(const ConnectionPool &&) = delete;

  // Destructor.
  ~ConnectionPool();

  // Initialization.
  void Initialize(const std::string &dir_path);

  // Executes a statement
  bool ExecuteStatement(const std::string &shard_suffix,
                        const std::string &statement);

 private:
  std::string dir_path_;
  // We always keep an open connection to the main non-sharded database.
  sqlite3 *main_connection_;
  // Default no-op callback.
  static int default_callback(void *NotUsed, int argc, char **argv,
                              char **azColName) {
    return 0;
  }
};

}  // namespace sqlconnections
}  // namespace shards

#endif  // SHARDS_SQLCONNECTIONS_POOL_
