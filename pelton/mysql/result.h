#ifndef PELTON_MYSQL_RESULT_H_
#define PELTON_MYSQL_RESULT_H_

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "mysql-cppconn-8/mysqlx/xdevapi.h"
// TODO(babmen): fix this.
#undef PRAGMA

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"

namespace pelton {
namespace mysql {

// Our version of an (abstract) SqlResult.
// This class provides functions to iterate through the records of a result
// in addition to the schema.
class SqlResult {
 public:
  class SqlResultImpl {
   public:
    virtual ~SqlResultImpl() = default;

    virtual bool hasData() = 0;
    virtual dataflow::Record fetchOne(const dataflow::SchemaRef &schema) = 0;
    virtual bool isInlined() const = 0;
  };

  class SqlResultIterator {
   public:
    // Iterator traits.
    using difference_type = int64_t;
    using value_type = dataflow::Record;
    using iterator_category = std::input_iterator_tag;
    using pointer = dataflow::Record *;
    using reference = dataflow::Record &;

    // Construct the generic iterator by providing a concrete implemenation.
    explicit SqlResultIterator(const dataflow::SchemaRef &schema)
        : impl_(nullptr), record_(schema, true) {}

    explicit SqlResultIterator(SqlResult::SqlResultImpl *impl,
                               const dataflow::SchemaRef &schema)
        : impl_(impl), record_(schema, true) {
      ++(*this);
    }

    // All operations translate to operation on the underlying implementation.
    SqlResultIterator &operator++() {
      if (this->impl_ && this->impl_->hasData()) {
        this->record_ = this->impl_->fetchOne(this->record_.schema());
      } else {
        this->impl_ = nullptr;
      }
      return *this;
    }
    bool operator==(const SqlResultIterator &o) const {
      return this->impl_ == o.impl_;
    }
    bool operator!=(const SqlResultIterator &o) const {
      return this->impl_ != o.impl_;
    }
    const dataflow::Record &operator*() const { return this->record_; }
    reference operator*() { return this->record_; }

   private:
    SqlResult::SqlResultImpl *impl_;
    dataflow::Record record_;
  };

  // Constructor
  SqlResult() = default;

  explicit SqlResult(std::unique_ptr<SqlResult::SqlResultImpl> &&impl,
                     const dataflow::SchemaRef &schema)
      : impl_(std::move(impl)), schema_(schema) {}

  bool hasData() { return this->impl_ && this->impl_->hasData(); }
  dataflow::SchemaRef getSchema() const { return this->schema_; }
  dataflow::Record fetchOne() { return this->impl_->fetchOne(this->schema_); }

  // Appends the provided SqlResult to this SqlResult, appeneded
  // result is moved and becomes empty after append.
  void Append(SqlResult &&other);
  void AppendDeduplicate(SqlResult &&other);
  void MakeInline();

  SqlResult::SqlResultIterator begin() {
    return SqlResult::SqlResultIterator{this->impl_.get(), this->schema_};
  }
  SqlResult::SqlResultIterator end() {
    return SqlResult::SqlResultIterator{this->schema_};
  }

  std::vector<dataflow::Record> Vectorize() {
    std::vector<dataflow::Record> result;
    for (dataflow::Record &record : *this) {
      result.push_back(std::move(record));
    }
    return result;
  }

 private:
  std::unique_ptr<SqlResult::SqlResultImpl> impl_;
  dataflow::SchemaRef schema_;
};

// Wrapper around an unmodified mysqlx::Result.
class MySqlResult : public SqlResult::SqlResultImpl {
 public:
  explicit MySqlResult(mysqlx::SqlResult &&result)
      : result_(std::move(result)) {}

  bool hasData() override;
  dataflow::Record fetchOne(const dataflow::SchemaRef &schema) override;
  bool isInlined() const override;

 private:
  mysqlx::SqlResult result_;
};

// Wrapper around a mysqlx::Result that is augmented with an additional value at
// some column.
class AugmentedSqlResult : public SqlResult::SqlResultImpl {
 public:
  AugmentedSqlResult(mysqlx::SqlResult &&result, size_t aug_index,
                     mysqlx::Value &&aug_value)
      : result_(std::move(result)),
        aug_index_(aug_index),
        aug_value_(std::make_unique<mysqlx::Value>(std::move(aug_value))) {}

  bool hasData() override;
  dataflow::Record fetchOne(const dataflow::SchemaRef &schema) override;
  bool isInlined() const override;

 private:
  mysqlx::SqlResult result_;
  size_t aug_index_;
  std::unique_ptr<mysqlx::Value> aug_value_;
};

// A result set with inlined values.
class InlinedSqlResult : public SqlResult::SqlResultImpl {
 public:
  explicit InlinedSqlResult(std::vector<dataflow::Record> &&records)
      : records_(std::move(records)), index_(0) {}

  bool hasData() override;
  dataflow::Record fetchOne(const dataflow::SchemaRef &schema) override;
  bool isInlined() const override;

  // For deduplication: create an unordered set that can be checked to
  // determine duplicates.
  std::unordered_set<std::string> RowSet() const;
  void AddRecord(dataflow::Record &&record);

 private:
  std::vector<dataflow::Record> records_;
  size_t index_;
};

}  // namespace mysql
}  // namespace pelton

#endif  // PELTON_MYSQL_RESULT_H_
