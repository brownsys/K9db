#ifndef PELTON_MYSQL_RESULT_H_
#define PELTON_MYSQL_RESULT_H_

#include <list>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "mysql-cppconn-8/mysqlx/xdevapi.h"
// TODO(babmen): fix this.
#undef PRAGMA

#include "pelton/mysql/column.h"
#include "pelton/mysql/row.h"

namespace pelton {
namespace mysql {

// Our version of Mysql's mysqlx::SqlResult.
// https://dev.mysql.com/doc/dev/connector-cpp/8.0/class_sql_result.html
class SqlResult {
 public:
  class SqlResultImpl {
   public:
    virtual bool hasData() const = 0;
    virtual uint64_t getAutoIncrementValue() = 0;

    virtual size_t getColumnCount() const = 0;
    virtual Column getColumn(size_t i) const = 0;

    virtual Row fetchOne() = 0;
    virtual size_t count() = 0;
    virtual bool isInlined() const = 0;
    virtual bool isNested() const = 0;
  };

  // Empty results.
  SqlResult() : impl_() {}

  explicit SqlResult(std::unique_ptr<SqlResult::SqlResultImpl> impl)
      : impl_(std::move(impl)) {}

  bool hasData() const { return this->impl_ && this->impl_->hasData(); }
  uint64_t getAutoIncrementValue() {
    return this->impl_->getAutoIncrementValue();
  }

  size_t getColumnCount() const { return this->impl_->getColumnCount(); }
  Column getColumn(size_t i) const { return this->impl_->getColumn(i); }

  Row fetchOne() { return this->impl_->fetchOne(); }
  size_t count() { return this->impl_->count(); }

  // Appends the provided SqlResult to this SqlResult, appeneded
  // result is moved and becomes empty after append.
  void Append(SqlResult &&other);
  void AppendDeduplicate(SqlResult &&other);

 private:
  std::unique_ptr<SqlResult::SqlResultImpl> impl_;
};

// Wrapper around an unmodified mysqlx::Result.
class MySqlResult : public SqlResult::SqlResultImpl {
 public:
  explicit MySqlResult(mysqlx::SqlResult &&result)
      : result_(std::move(result)) {}

  bool hasData() const override;
  uint64_t getAutoIncrementValue() override;

  size_t getColumnCount() const override;
  Column getColumn(size_t i) const override;

  Row fetchOne() override;
  size_t count() override;
  bool isInlined() const override;
  bool isNested() const override;

 private:
  mysqlx::SqlResult result_;
};

// Wrapper around a mysqlx::Result that is augmented with an additional value at
// some column.
class AugmentedSqlResult : public SqlResult::SqlResultImpl {
 public:
  AugmentedSqlResult(mysqlx::SqlResult &&result, size_t aug_index,
                     mysqlx::Value &&aug_value, Column aug_column)
      : result_(std::move(result)),
        aug_index_(aug_index),
        aug_value_(std::move(aug_value)),
        aug_column_(aug_column) {}

  bool hasData() const override;
  uint64_t getAutoIncrementValue() override;

  size_t getColumnCount() const override;
  Column getColumn(size_t i) const override;

  Row fetchOne() override;
  size_t count() override;
  bool isInlined() const override;
  bool isNested() const override;

 private:
  mysqlx::SqlResult result_;
  size_t aug_index_;
  mysqlx::Value aug_value_;
  Column aug_column_;
};

// A result set with inlined values.
class InlinedSqlResult : public SqlResult::SqlResultImpl {
 public:
  InlinedSqlResult(std::vector<Row> &&rows, std::vector<Column> cols)
      : rows_(std::move(rows)), cols_(std::move(cols)), index_(0) {}

  bool hasData() const override;
  uint64_t getAutoIncrementValue() override;

  size_t getColumnCount() const override;
  Column getColumn(size_t i) const override;

  Row fetchOne() override;
  size_t count() override;
  bool isInlined() const override;
  bool isNested() const override;

  // For deduplication: create an unordered set that can be checked to
  // determine duplicates.
  std::unordered_set<std::string> RowSet() const;
  void AddRow(Row &&row);

 private:
  std::vector<Row> rows_;
  std::vector<Column> cols_;
  size_t index_;
};

// A composite result set created by multiple inner result sets.
class NestedSqlResult : public SqlResult::SqlResultImpl {
 public:
  NestedSqlResult() : results_(), count_(0) {}

  bool hasData() const override;
  uint64_t getAutoIncrementValue() override;

  size_t getColumnCount() const override;
  Column getColumn(size_t i) const override;

  Row fetchOne() override;
  size_t count() override;
  bool isInlined() const override;
  bool isNested() const override;

  // Appending and Prepending.
  void Append(std::unique_ptr<SqlResultImpl> &&other);
  void Prepend(std::unique_ptr<SqlResultImpl> &&other);

 private:
  std::list<std::unique_ptr<SqlResultImpl>> results_;
  size_t count_;
};

}  // namespace mysql
}  // namespace pelton

#endif  // PELTON_MYSQL_RESULT_H_
