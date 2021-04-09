// Manages mysql connections to the different shard/mini-databases.
#ifndef PELTON_SHARDS_POOL_H_
#define PELTON_SHARDS_POOL_H_

#include <string>
#include <unordered_map>
#include <unordered_set>

#include "absl/status/status.h"
#include "pelton/shards/types.h"
#include "pelton/types.h"

namespace pelton {
namespace shards {

class ConnectionPool {
 public:
  // Constructor.
  ConnectionPool() : session_(nullptr) {}

  // Not copyable or movable.
  ConnectionPool(const ConnectionPool &) = delete;
  ConnectionPool &operator=(const ConnectionPool &) = delete;
  ConnectionPool(const ConnectionPool &&) = delete;
  ConnectionPool &operator=(const ConnectionPool &&) = delete;

  // Destructor.
  ~ConnectionPool();

  // Initialize (when the state is initialized).
  void Initialize(const std::string &username, const std::string &password);

  // Execute statement against the default un-sharded database.
  SqlResult ExecuteDefault(const std::string &sql);

  // Execute statement against given user shard(s).
  SqlResult ExecuteShard(const std::string &sql,
                         const ShardingInformation &info,
                         const UserId &user_id);
  SqlResult ExecuteShards(const std::string &sql,
                          const ShardingInformation &info,
                          const std::unordered_set<UserId> &user_ids);

  void RemoveShard(const std::string &shard_name);

 private:
  // Connection management.
  void *session_;

  // Manage using shards in session_.
  void OpenDefaultShard();
  void OpenShard(const ShardKind &shard_kind, const UserId &user_id);
};

}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_POOL_H_
