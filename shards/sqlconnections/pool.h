// Manages sqlite3 connections to the different shard/mini-databases.

#ifndef SHARDS_SQLCONNECTIONS_POOL_H_
#define SHARDS_SQLCONNECTIONS_POOL_H_

#include <sqlite3.h>

#include <functional>
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
                        const std::string &statement,
                        std::function<void(int *, char ***, char ***)> modifier,
                        char **errmsg);

  void FlushBuffer(std::function<int(void *, int, char **, char **)> cb,
                   void *context, char **errmsg);

 private:
  std::string dir_path_;
  // We always keep an open connection to the main non-sharded database.
  sqlite3 *main_connection_;
};

}  // namespace sqlconnections
}  // namespace shards

#endif  // SHARDS_SQLCONNECTIONS_POOL_H_
