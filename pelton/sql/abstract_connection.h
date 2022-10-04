// An abstract connection to some database.
#ifndef PELTON_SQL_ABSTRACT_CONNECTION_H_
#define PELTON_SQL_ABSTRACT_CONNECTION_H_

#include <cstdint>
#include <string>

#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

#define DEFAULT_SHARD "__default"

namespace pelton {
namespace sql {

class AbstractConnection {
 public:
  AbstractConnection() = default;
  virtual ~AbstractConnection() = default;

  // Close the connection.
  virtual void Open(const std::string &db_name) = 0;
  virtual void Close() = 0;

  // Statements.
  virtual bool ExecuteCreateTable(const sqlast::CreateTable &sql,
                                  size_t copies) = 0;

  virtual bool ExecuteCreateIndex(const sqlast::CreateIndex &sql) = 0;

  // Insert.
  virtual int ExecuteInsert(const sqlast::Insert &sql,
                            const std::string &shard_name, size_t copy) = 0;

  // Delete.
  virtual SqlResultSet ExecuteDelete(const sqlast::Delete &sql) = 0;

  // Selects.
  virtual SqlResultSet ExecuteSelect(const sqlast::Select &sql) const = 0;
  virtual SqlResultSet GetShard(const std::string &table_name,
                                const std::string &shard_name) const = 0;

  // Everything in a table.
  virtual SqlResultSet GetAll(const std::string &table_name) const = 0;
  virtual SqlResultSet GetAll(const std::string &table_name,
                              size_t copy) const = 0;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ABSTRACT_CONNECTION_H_
