// Keeps a connection to the underlying database and manages execution of
// SQL commands against it.
#include "pelton/sql/eager_executor.h"

#include "glog/logging.h"
#include "pelton/util/perf.h"

namespace pelton {
namespace sql {

// MariaDB API types.
using DBProperties = ::sql::ConnectOptionsMap;
using DBDriver = ::sql::Driver;
using DBConnection = ::sql::Connection;
using DBStatement = ::sql::Statement;
using DBResultSet = ::sql::ResultSet;

// Initialize: keep a connection open to the underlying DB.
void SqlEagerExecutor::Initialize(const std::string &username,
                                  const std::string &password) {
  DBProperties props;
  props["hostName"] = "localhost";
  props["userName"] = username;
  props["password"] = password;
  // props["CLIENT_MULTI_STATEMENTS"] = true;

  DBDriver *driver = ::sql::mariadb::get_driver_instance();
  this->conn_ = std::unique_ptr<DBConnection>(driver->connect(props));
  this->stmt_ = std::unique_ptr<DBStatement>(this->conn_->createStatement());

  // Create and use the DB.
  this->stmt_->execute("CREATE DATABASE IF NOT EXISTS pelton");
  this->stmt_->execute("USE pelton");
  // this->stmt_->execute("SET GLOBAL table_open_cache=50000");
  // this->stmt_->execute("SET GLOBAL schema_definition_cache=10000");
  // this->stmt_->execute("SET GLOBAL table_definition_cache=10000");
}

// Execute statement against the underlying database.
// Sharding information should already be baked in the SQL command.
bool SqlEagerExecutor::ExecuteStatement(const std::string &sql) {
#ifndef PELTON_OPT
  LOG(INFO) << "Executing Statement:" << sql;
#endif
  perf::Start("SQL");
  this->stmt_->execute(sql);
  perf::End("SQL");
  return true;
}

int SqlEagerExecutor::ExecuteUpdate(const std::string &sql) {
#ifndef PELTON_OPT
  LOG(INFO) << "Executing Statement:" << sql;
#endif
  perf::Start("SQL");
  int result = this->stmt_->executeUpdate(sql);
  perf::End("SQL");
  return result;
}

std::unique_ptr<DBResultSet> SqlEagerExecutor::ExecuteQuery(
    const std::string &sql) {
#ifndef PELTON_OPT
  LOG(INFO) << "Executing Statement:" << sql;
#endif
  perf::Start("SQL");
  DBResultSet *result = this->stmt_->executeQuery(sql);
  perf::End("SQL");
  return std::unique_ptr<DBResultSet>(result);
}

}  // namespace sql
}  // namespace pelton
