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
  const std::string &getColumnName() const;

 private:
  // https://github.com/mysql/mysql-connector-cpp/blob/857a8d63d817a17160ca6062ceef6e9d6d0dd128/include/mysqlx/common_constants.h#L204
  mysqlx::Type type_;
  std::string name_;
};

// Our version of a single row in a query result.
// https://dev.mysql.com/doc/dev/connector-cpp/8.0/classmysqlx_1_1abi2_1_1r0_1_1_row.html
class Row {
 public:
  Row(const std::vector<size_t> &aug_indices,
      const std::vector<mysqlx::Value> &aug_values)
      : Row({}, aug_indices, aug_values) {}

  Row(mysqlx::Row &&row, const std::vector<size_t> &aug_indices,
      const std::vector<mysqlx::Value> &aug_values);

  const mysqlx::Value &get(size_t pos) const;
  const mysqlx::Value &operator[](size_t pos) const;
  bool isNull() const;

 private:
  mysqlx::Row row_;
  const std::vector<size_t> &aug_indices_;
  const std::vector<mysqlx::Value> &aug_values_;
};

// Our version of Mysql's mysqlx::SqlResult.
// https://dev.mysql.com/doc/dev/connector-cpp/8.0/class_sql_result.html
class SqlResult {
 public:
  SqlResult() : SqlResult(std::vector<size_t>{}, {}) {}

  SqlResult(size_t aug_index, Column aug_column)
      : SqlResult(std::vector<size_t>{aug_index}, {aug_column}) {}

  // Sorted by indices.
  SqlResult(std::vector<size_t> &&aug_indices, std::vector<Column> &&aug_cols);

  // Add a wrapped mysql result with no augmented values.
  void AddResult(mysqlx::SqlResult &&result);

  // Add augmented value(s) with no wrapped mysql result.
  void AddAugResult(const mysqlx::Value &value) { this->AddAugResult({value}); }
  void AddAugResult(mysqlx::Value &&value) {
    this->AddAugResult({std::move(value)});
  }
  void AddAugResult(std::vector<mysqlx::Value> &&value) {
    this->count_++;
    this->aug_values_.push_back(std::move(value));
  }

  // Add a wrapped mysql result with augmented value(s).
  void AddResult(mysqlx::SqlResult &&result, const mysqlx::Value &value) {
    this->AddResult(std::move(result), std::vector<mysqlx::Value>{value});
  }
  void AddResult(mysqlx::SqlResult &&result, mysqlx::Value &&value) {
    this->AddResult(std::move(result),
                    std::vector<mysqlx::Value>{std::move(value)});
  }
  void AddResult(mysqlx::SqlResult &&result,
                 std::vector<mysqlx::Value> &&values) {
    this->count_ += result.count();
    this->results_.push_back(std::move(result));
    this->aug_values_.push_back(std::move(values));
  }

  // Merge/append another SqlResult to this.
  void Consume(SqlResult *other);

  // mysqlx::SqlResult API.
  bool hasData() const;

  uint64_t getAutoIncrementValue();

  size_t getColumnCount() const;
  Column getColumn(size_t i) const;

  Row fetchOne();
  size_t count() const;

 private:
  // Used to determine which row to fetch next and how many are left.
  size_t result_index_;
  size_t count_;
  // Inlined columns/values. These are used to augment the result with values
  // that are not stored in the database physically (e.g. the shard_by column).
  std::vector<size_t> aug_indices_;
  std::vector<Column> aug_columns_;
  // aug_values_[x][i] corresponds to aug_indices_[i] and aug_columns_[i].
  // aug_values_[i] corresponds to all rows in results_[i].
  std::vector<std::vector<mysqlx::Value>> aug_values_;
  // Wrapped mysql results.
  std::vector<mysqlx::SqlResult> results_;
};

}  // namespace pelton

#endif  // PELTON_TYPES_H_
