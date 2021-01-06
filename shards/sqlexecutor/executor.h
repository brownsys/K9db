// Manages sqlite3 connections to the different shard/mini-databases.

#ifndef SHARDS_SQLEXECUTOR_EXECUTOR_H_
#define SHARDS_SQLEXECUTOR_EXECUTOR_H_

#include <sqlite3.h>

#include <functional>
#include <memory>
#include <string>

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

  // Executes a statement
  bool ExecuteStatement(std::unique_ptr<ExecutableStatement> statement,
                        Callback callback, void *context, char **errmsg) const;

 private:
  std::string dir_path_;
  // We always keep an open connection to the main non-sharded database.
  ::sqlite3 *default_noshard_connection_;
};

}  // namespace sqlexecutor
}  // namespace shards

#endif  // SHARDS_SQLEXECUTOR_EXECUTOR_H_
