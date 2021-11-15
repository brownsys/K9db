// Keeps a connection to the underlying database and manages execution of
// SQL commands against it.
#ifndef PELTON_SQL_EAGER_EXECUTOR_H_
#define PELTON_SQL_EAGER_EXECUTOR_H_

#include <memory>
#include <string>
#include <shared_mutex>

#include "mariadb/conncpp.hpp"

namespace pelton {
namespace sql {

class SqlEagerExecutor {
 public:
  SqlEagerExecutor() = default;

  // Not copyable or movable.
  SqlEagerExecutor(const SqlEagerExecutor &) = delete;
  SqlEagerExecutor &operator=(const SqlEagerExecutor &) = delete;
  SqlEagerExecutor(const SqlEagerExecutor &&) = delete;
  SqlEagerExecutor &operator=(const SqlEagerExecutor &&) = delete;

  // Initialize: keep a connection open to the underlying DB.
  void Initialize(const std::string &db_name, const std::string &username,
                  const std::string &password);

  // Execute statement against the underlying database.
  // Sharding information should already be baked in the SQL command.
  bool ExecuteStatement(const std::string &sql);
  int ExecuteUpdate(const std::string &sql);
  std::unique_ptr<::sql::ResultSet> ExecuteQuery(const std::string &sql);

 private:
  // Connection management.
  std::unique_ptr<::sql::Connection> conn_;
  std::unique_ptr<::sql::Statement> stmt_;
  static std::shared_mutex MTX;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_EAGER_EXECUTOR_H_
