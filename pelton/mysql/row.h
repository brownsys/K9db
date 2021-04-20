#ifndef PELTON_MYSQL_ROW_H_
#define PELTON_MYSQL_ROW_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "mysql-cppconn-8/mysqlx/xdevapi.h"
// TODO(babmen): fix this.
#undef PRAGMA

namespace pelton {
namespace mysql {

// Our version of a single row in a query result.
// https://dev.mysql.com/doc/dev/connector-cpp/8.0/classmysqlx_1_1abi2_1_1r0_1_1_row.html
class Row {
 public:
  // Parent class for abstracting away the implemenetation details of Row.
  class AbstractRowImpl {
   public:
    virtual const mysqlx::Value &get(size_t post) const = 0;
  };

  explicit Row(std::unique_ptr<Row::AbstractRowImpl> &&impl)
      : impl_(std::move(impl)) {}

  const mysqlx::Value &get(size_t pos) const { return this->impl_->get(pos); }
  const mysqlx::Value &operator[](size_t pos) const {
    return this->impl_->get(pos);
  }

  std::string StringRepr(size_t colnum) const;

 private:
  std::unique_ptr<Row::AbstractRowImpl> impl_;
};

// Wrapper around an unmodified mysqlx::Row.
class MySqlRow : public Row::AbstractRowImpl {
 public:
  explicit MySqlRow(mysqlx::Row &&row) : row_(std::move(row)) {}

  const mysqlx::Value &get(size_t pos) const override;

 private:
  mysqlx::Row row_;
};

// Wrapper around a mysqlx::Row that is augmented with an additional value at
// some index.
class AugmentedRow : public Row::AbstractRowImpl {
 public:
  AugmentedRow(mysqlx::Row &&row, size_t aug_index,
               const mysqlx::Value &aug_value)
      : row_(std::move(row)),
        aug_index_(aug_index),
        aug_value_(std::move(aug_value)) {}

  const mysqlx::Value &get(size_t pos) const override;

 private:
  mysqlx::Row row_;
  size_t aug_index_;
  mysqlx::Value aug_value_;
};

// A row with inlined values.
class InlinedRow : public Row::AbstractRowImpl {
 public:
  explicit InlinedRow(std::vector<mysqlx::Value> &&values)
      : values_(std::move(values)) {}

  explicit InlinedRow(const Row &row, size_t colnum);

  const mysqlx::Value &get(size_t pos) const override;

 private:
  std::vector<mysqlx::Value> values_;
};

}  // namespace mysql
}  // namespace pelton

#endif  // PELTON_MYSQL_ROW_H_
