// Manages sqlite3 connections to the different shard/mini-databases.

#ifndef SHARDS_SQLEXECUTOR_EXECUTOR_H_
#define SHARDS_SQLEXECUTOR_EXECUTOR_H_

#include <sqlite3.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_set>

#include "shards/sqlexecutor/executable.h"

namespace shards {
namespace sqlexecutor {

class SQLExecutor {
 public:
  // Constructor.
  SQLExecutor() {}

  // Not copyable or movable.
  SQLExecutor(const SQLExecutor &) = delete;
  SQLExecutor &operator=(const SQLExecutor &) = delete;
  SQLExecutor(const SQLExecutor &&) = delete;
  SQLExecutor &operator=(const SQLExecutor &&) = delete;

  // Destructor.
  ~SQLExecutor();

  void Initialize(const std::string &dir_path);

  // Executes a statement.
  void StartBlock();
  bool ExecuteStatement(std::unique_ptr<ExecutableStatement> statement,
                        Callback callback, void *context, char **errmsg);

 private:
  std::string dir_path_;
  // We always keep an open connection to the main non-sharded database.
  ::sqlite3 *default_noshard_connection_;
  // Stores all records produced within a block for deduplication.
  // A trie is more suitable here.
  std::unordered_set<std::string> deduplication_buffer_;
};

}  // namespace sqlexecutor
}  // namespace shards

#endif  // SHARDS_SQLEXECUTOR_EXECUTOR_H_
