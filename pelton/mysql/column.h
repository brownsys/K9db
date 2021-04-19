#ifndef PELTON_MYSQL_COLUMN_H_
#define PELTON_MYSQL_COLUMN_H_

#include <string>
#include <utility>
#include <vector>

#include "mysql-cppconn-8/mysqlx/xdevapi.h"
// TODO(babmen): fix this.
#undef PRAGMA

namespace pelton {
namespace mysql {

// Our version of a column, includes the column name and type.
// https://dev.mysql.com/doc/dev/connector-cpp/8.0/class_column.html
class Column {
 public:
  Column() = default;

  explicit Column(const mysqlx::Column &col);

  Column(mysqlx::Type type, const std::string &name);

  mysqlx::Type getType() const;
  const std::string &getColumnName() const;

 private:
  // https://github.com/mysql/mysql-connector-cpp/blob/857a8d63d817a17160ca6062ceef6e9d6d0dd128/include/mysqlx/common_constants.h#L204
  mysqlx::Type type_;
  std::string name_;
};

}  // namespace mysql
}  // namespace pelton

#endif  // PELTON_MYSQL_COLUMN_H_
