// Manages sqlite3 connections to the different shard/mini-databases.
#ifndef PELTON_SHARDS_POOL_H_
#define PELTON_SHARDS_POOL_H_

#include <sqlite3.h>

#include <string>
#include <unordered_set>

#include "pelton/shards/types.h"

namespace pelton {
namespace shards {

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

  // Initialize (when the state is initialized).
  void Initialize(const std::string &dir_path);

  // Execute statement against the default un-sharded database.
  bool ExecuteDefault(const std::string &sql, const Callback &callback,
                      void *context, char **errmsg);

  // Execute statement against given user shard(s).
  bool ExecuteShard(const std::string &sql, const ShardKind &shard_kind,
                    const UserId &user_id, const Callback &callback,
                    void *context, char **errmsg);
  bool ExecuteShard(const std::string &sql, const ShardKind &shard_kind,
                    const std::unordered_set<UserId> &user_ids,
                    const Callback &callback, void *context, char **errmsg);

 private:
  // Connection management.
  ::sqlite3 *GetDefaultConnection() const;
  ::sqlite3 *GetConnection(const ShardKind &shard_kind,
                           const UserId &user_id) const;

  // Actually execute statements after their connection has been resolved.
  bool Execute(const std::string &sql, ::sqlite3 *connection,
               const Callback &callback, void *context, char **errmsg);

  std::string dir_path_;
  // We always keep an open connection to the main non-sharded database.
  ::sqlite3 *default_noshard_connection_;
};

}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_POOL_H_
