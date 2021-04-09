// Our version of mysqlx API types.
#ifndef PELTON_TYPES_H_
#define PELTON_TYPES_H_

#include <string>
#include <utility>
#include <vector>

#include "mysql-cppconn-8/mysqlx/xdevapi.h"
// TODO(babmen): fix this.
#undef PRAGMA

namespace pelton {

// Our version of a column, includes the column name and type.
// https://dev.mysql.com/doc/dev/connector-cpp/8.0/class_column.html
class Column {
 public:
  Column() = default;

  explicit Column(const mysqlx::Column &col);

  Column(mysqlx::Type type, const std::string &name);

  mysqlx::Type getType() const;
  const mysqlx::string &getColumnName() const;

 private:
  // https://github.com/mysql/mysql-connector-cpp/blob/857a8d63d817a17160ca6062ceef6e9d6d0dd128/include/mysqlx/common_constants.h#L204
  mysqlx::Type type_;
  mysqlx::string name_;
};

// Our version of a single row in a query result.
// https://dev.mysql.com/doc/dev/connector-cpp/8.0/classmysqlx_1_1abi2_1_1r0_1_1_row.html
class Row {
 public:
  Row(mysqlx::Row &&row, int index, const mysqlx::Value &value);

  const mysqlx::Value &get(size_t pos);
  const mysqlx::Value &operator[](size_t pos) const;
  bool isNull() const;

 private:
  mysqlx::Row row_;
  int augmented_index_;
  const mysqlx::Value &augmented_value_;
};

// Our version of Mysql's mysqlx::SqlResult.
// https://dev.mysql.com/doc/dev/connector-cpp/8.0/class_sql_result.html
class SqlResult {
 public:
  SqlResult();
  SqlResult(int augmented_index, Column augmented_column);

  // Add result and no augmented value.
  void AddResult(mysqlx::SqlResult &&result);

  // Add result with augmented value.
  template <typename V>
  void AddResult(mysqlx::SqlResult &&result, const V &value) {
    this->count_ += result.count();
    this->results_.push_back(std::move(result));
    this->augmented_values_.emplace_back(value);
  }
  template <typename V>
  void AddResult(mysqlx::SqlResult &&result, V &&value) {
    this->count_ += result.count();
    this->results_.push_back(std::move(result));
    this->augmented_values_.emplace_back(std::move(value));
  }

  void Consume(SqlResult *other) {
    for (size_t i = 0; i < other->results_.size(); i++) {
      this->results_.push_back(std::move(other->results_.at(i)));
      this->augmented_values_.push_back(
          std::move(other->augmented_values_.at(i)));
    }
  }

  // mysqlx::SqlResult API.
  bool hasData();

  uint64_t getAutoIncrementValue();

  size_t getColumnCount() const;
  Column getColumn(size_t i) const;

  Row fetchOne();
  size_t count() const;

 private:
  size_t result_index_;
  size_t count_;
  int augmented_index_;
  Column augmented_column_;
  std::vector<mysqlx::Value> augmented_values_;
  std::vector<mysqlx::SqlResult> results_;
};

}  // namespace pelton

#endif  // PELTON_TYPES_H_
