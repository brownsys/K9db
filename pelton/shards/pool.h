// Manages mysql connections to the different shard/mini-databases.
#ifndef PELTON_SHARDS_POOL_H_
#define PELTON_SHARDS_POOL_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "absl/status/status.h"
#include "mariadb/conncpp.hpp"
#include "pelton/mysql/result.h"
#include "pelton/shards/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {

class ConnectionPool {
 public:
  ConnectionPool() = default;

  // Not copyable or movable.
  ConnectionPool(const ConnectionPool &) = delete;
  ConnectionPool &operator=(const ConnectionPool &) = delete;
  ConnectionPool(const ConnectionPool &&) = delete;
  ConnectionPool &operator=(const ConnectionPool &&) = delete;

  // Initialize (when the state is initialized).
  void Initialize(const std::string &username, const std::string &password);

  // Execute statement against the default un-sharded database.
  mysql::SqlResult ExecuteDefault(const sqlast::AbstractStatement *sql,
                                  const dataflow::SchemaRef &schema = {});

  // Execute statement against given user shard(s).
  mysql::SqlResult ExecuteShard(const sqlast::AbstractStatement *sql,
                                const ShardingInformation &info,
                                const UserId &user_id,
                                const dataflow::SchemaRef &schema = {});

  mysql::SqlResult ExecuteShards(const sqlast::AbstractStatement *sql,
                                 const ShardingInformation &info,
                                 const std::unordered_set<UserId> &user_ids,
                                 const dataflow::SchemaRef &schema = {});

  void RemoveShard(const std::string &shard_name);

 private:
  mysql::SqlResult ExecuteMySQL(const sqlast::AbstractStatement *sql,
                                const dataflow::SchemaRef &schema,
                                const std::string &shard_name = "default_db",
                                int aug_index = -1,
                                const std::string &aug_value = "");

  // Connection management.
  std::unique_ptr<sql::Connection> conn_;
  std::unique_ptr<sql::Statement> stmt_;
};

}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_POOL_H_
