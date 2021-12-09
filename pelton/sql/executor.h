// Intermediate layer between pelton and whatever underlying DB we use.
#ifndef PELTON_SQL_EXECUTOR_H_
#define PELTON_SQL_EXECUTOR_H_

#include <memory>
#include <string>
#include <unordered_set>

#include "gtest/gtest_prod.h"
#include "pelton/dataflow/schema.h"
#include "pelton/shards/types.h"
#include "pelton/sql/connection.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

#define __UNSHARDED_DB "default_db"
#define __NO_AUG -1
#define __NO_AUG_VALUE ""

namespace pelton {
namespace sql {

class PeltonExecutor {
 public:
  PeltonExecutor() = default;
  ~PeltonExecutor() { this->Close(); }

  // Not copyable or movable.
  PeltonExecutor(const PeltonExecutor &) = delete;
  PeltonExecutor &operator=(const PeltonExecutor &) = delete;
  PeltonExecutor(const PeltonExecutor &&) = delete;
  PeltonExecutor &operator=(const PeltonExecutor &&) = delete;

  // Initialization: initialize the eager executor so that it maintains
  // an open connection to the underlying DB.
  void Initialize(const std::string &db_name, const std::string &username,
                  const std::string &password);

  // Close the connection to the DB.
  void Close() {
    if (this->connection_ != nullptr) {
      this->connection_->Close();
      this->connection_ = nullptr;
    }
  }

  // For singleton connections.
  static void CloseAll();

  // Execute statement against the default un-sharded database.
  SqlResult Default(const sqlast::AbstractStatement *sql,
                    const dataflow::SchemaRef &schema = {});

  // Execute statement against given user shard.
  SqlResult Shard(const sqlast::AbstractStatement *sql,
                  const std::string &shard_kind, const shards::UserId &user_id,
                  const dataflow::SchemaRef &schema = {}, int aug_index = -1);

  // Execute statement against given user shards.
  SqlResult Shards(const sqlast::AbstractStatement *sql,
                   const std::string &shard_kind,
                   const std::unordered_set<shards::UserId> &user_ids,
                   const dataflow::SchemaRef &schema = {}, int aug_index = -1);

 private:
  SqlResult EmptyResult(const sqlast::AbstractStatement *sql,
                        const dataflow::SchemaRef &schema);

  SqlResult Execute(const sqlast::AbstractStatement *sql,
                    const dataflow::SchemaRef &schema,
                    const std::string &shard_name = "default_db",
                    int aug_index = -1, const std::string &aug_value = "");

  // The connection to the underlying DB.
  std::unique_ptr<PeltonConnection> connection_;

  /*
  FRIEND_TEST(LazyExecutorTest, TestCreateTableDefault);
  FRIEND_TEST(LazyExecutorTest, TestCreateTableShard);
  FRIEND_TEST(LazyExecutorTest, TestUpdatesDefault);
  FRIEND_TEST(LazyExecutorTest, TestUpdatesShard);
  FRIEND_TEST(LazyExecutorTest, TestSelectDefault);
  FRIEND_TEST(LazyExecutorTest, TestSelectShard);
  FRIEND_TEST(LazyExecutorTest, TestSelectShardSet);
  */
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_EXECUTOR_H_
