// Interface between pelton and the underlying backend database.
// Calls to Execute... return our own SqlResult class.
// This class contains different information depending on the type
// of query being executed. It may contain data directly inlined in the
// result, or it may be a lazy wrapper that stores information about the
// underlying queries, and executes them in some order as data is read from
// the resutl.
#ifndef PELTON_SQL_LAZY_EXECUTOR_H_
#define PELTON_SQL_LAZY_EXECUTOR_H_

#include <string>
#include <unordered_set>

#include "gtest/gtest_prod.h"
#include "pelton/shards/types.h"
#include "pelton/sql/eager_executor.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace sql {

class SqlLazyExecutor {
 public:
  SqlLazyExecutor() = default;

  // Not copyable or movable.
  SqlLazyExecutor(const SqlLazyExecutor &) = delete;
  SqlLazyExecutor &operator=(const SqlLazyExecutor &) = delete;
  SqlLazyExecutor(const SqlLazyExecutor &&) = delete;
  SqlLazyExecutor &operator=(const SqlLazyExecutor &&) = delete;

  // Initialization: initialize the eager executor so that it maintains
  // an open connection to the underlying DB.
  void Initialize(const std::string &db_name, const std::string &username,
                  const std::string &password);

  // Execute statement against the default un-sharded database.
  SqlResult ExecuteDefault(const sqlast::AbstractStatement *sql,
                           const dataflow::SchemaRef &schema = {});

  // Execute statement against given user shard.
  SqlResult ExecuteShard(const sqlast::AbstractStatement *sql,
                         const std::string &shard_kind,
                         const shards::UserId &user_id,
                         const dataflow::SchemaRef &schema = {},
                         int aug_index = -1);

  // Execute statement against given user shards.
  SqlResult ExecuteShards(const sqlast::AbstractStatement *sql,
                          const std::string &shard_kind,
                          const std::unordered_set<shards::UserId> &user_ids,
                          const dataflow::SchemaRef &schema = {},
                          int aug_index = -1);

 private:
  SqlResult EmptyResultByType(sqlast::AbstractStatement::Type type,
                              bool returning,
                              const dataflow::SchemaRef &schema);

  SqlResult Execute(const sqlast::AbstractStatement *sql,
                    const dataflow::SchemaRef &schema,
                    const std::string &shard_name = "default_db",
                    int aug_index = -1, const std::string &aug_value = "");

  // Eager executor is responsible for executing the SQL commands against
  // the backend database.
  SqlEagerExecutor eager_executor_;

  FRIEND_TEST(LazyExecutorTest, TestCreateTableDefault);
  FRIEND_TEST(LazyExecutorTest, TestCreateTableShard);
  FRIEND_TEST(LazyExecutorTest, TestUpdatesDefault);
  FRIEND_TEST(LazyExecutorTest, TestUpdatesShard);
  FRIEND_TEST(LazyExecutorTest, TestSelectDefault);
  FRIEND_TEST(LazyExecutorTest, TestSelectShard);
  FRIEND_TEST(LazyExecutorTest, TestSelectShardSet);
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_LAZY_EXECUTOR_H_
