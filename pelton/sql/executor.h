// Manages mysql connections to the different shard/mini-databases.
#ifndef PELTON_SQL_EXECUTOR_H_
#define PELTON_SQL_EXECUTOR_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "absl/status/status.h"
#include "mariadb/conncpp.hpp"
#include "pelton/shards/types.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace sql {

class SqlExecutor {
 public:
  SqlExecutor() = default;

  // Not copyable or movable.
  SqlExecutor(const SqlExecutor &) = delete;
  SqlExecutor &operator=(const SqlExecutor &) = delete;
  SqlExecutor(const SqlExecutor &&) = delete;
  SqlExecutor &operator=(const SqlExecutor &&) = delete;

  // Initialize (when the state is initialized).
  void Initialize(const std::string &username, const std::string &password);

  // Execute statement against the default un-sharded database.
  SqlResult ExecuteDefault(const sqlast::AbstractStatement *sql,
                           const dataflow::SchemaRef &schema = {});

  // Execute statement against given user shard(s).
  SqlResult ExecuteShard(const sqlast::AbstractStatement *sql,
                         const shards::ShardingInformation &info,
                         const shards::UserId &user_id,
                         const dataflow::SchemaRef &schema = {});

  SqlResult ExecuteShards(const sqlast::AbstractStatement *sql,
                          const shards::ShardingInformation &info,
                          const std::unordered_set<shards::UserId> &user_ids,
                          const dataflow::SchemaRef &schema = {});

 private:
  SqlResult ExecuteMySQL(const sqlast::AbstractStatement *sql,
                         const dataflow::SchemaRef &schema,
                         const std::string &shard_name = "default_db",
                         int aug_index = -1, const std::string &aug_value = "");

  // Connection management.
  std::unique_ptr<::sql::Connection> conn_;
  std::unique_ptr<::sql::Statement> stmt_;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_EXECUTOR_H_
