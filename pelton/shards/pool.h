// Manages sqlite3 connections to the different shard/mini-databases.
#ifndef PELTON_SHARDS_POOL_H_
#define PELTON_SHARDS_POOL_H_

#include <sqlite3.h>

#include <string>
#include <unordered_set>

#include "absl/status/status.h"
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
  absl::Status ExecuteDefault(const std::string &sql,
                              const OutputChannel &output);

  // Execute statement against given user shard(s).
  absl::Status ExecuteShard(const std::string &sql,
                            const ShardingInformation &info,
                            const UserId &user_id, const OutputChannel &output);
  absl::Status ExecuteShard(const std::string &sql,
                            const ShardingInformation &info,
                            const std::unordered_set<UserId> &user_ids,
                            const OutputChannel &output);

 private:
  // Connection management.
  ::sqlite3 *GetDefaultConnection() const;
  ::sqlite3 *GetConnection(const ShardKind &shard_kind,
                           const UserId &user_id) const;

  // Actually execute statements after their connection has been resolved.
  absl::Status ExecuteDefault(const std::string &sql, ::sqlite3 *connection,
                              const OutputChannel &output);
  absl::Status ExecuteShard(const std::string &sql, ::sqlite3 *connection,
                            const ShardingInformation &info,
                            const UserId &user_id, const OutputChannel &output);

  std::string dir_path_;
  // We always keep an open connection to the main non-sharded database.
  ::sqlite3 *default_noshard_connection_;

  // We use these private static fields to remove captures from std::function
  // and lambdas, so that they can be passed to SQLITE3 API as old C-style
  // func pointers.
  static const Callback *CALLBACK_NO_CAPTURE;
  static size_t SHARD_BY_INDEX_NO_CAPTURE;
  static char *SHARD_BY_NO_CAPTURE;
  static char *USER_ID_NO_CAPTURE;
};

}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_POOL_H_
