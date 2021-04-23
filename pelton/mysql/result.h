#ifndef PELTON_MYSQL_RESULT_H_
#define PELTON_MYSQL_RESULT_H_

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "mysql-cppconn-8/jdbc/cppconn/resultset.h"
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

    virtual bool IsStatement() const { return false; }
    virtual bool IsUpdate() const { return false; }
    virtual bool IsQuery() const { return false; }

    // Statement API.
    virtual bool Success() const { return false; }

    // Update API.
    virtual int UpdateCount() const { return -1; }

    // Query API.
    virtual bool HasNext() { return false; }
    virtual dataflow::Record FetchOne(const dataflow::SchemaRef &schema) {
      return dataflow::Record{schema, false};
    }
    virtual bool IsInlined() const {
      // Checks if the data has been pulled in to memory and is inlined.
      return false;
    }
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
    explicit SqlResultIterator(SqlResult::SqlResultImpl *impl,
                               const dataflow::SchemaRef &schema)
        : impl_(impl), record_(schema, true) {
      ++(*this);
    }

    // All operations translate to operation on the underlying implementation.
    SqlResultIterator &operator++() {
      if (this->impl_ && this->impl_->HasNext()) {
        this->record_ = this->impl_->FetchOne(this->record_.schema());
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
  SqlResult();

  explicit SqlResult(std::unique_ptr<SqlResult::SqlResultImpl> &&impl,
                     const dataflow::SchemaRef &schema = {})
      : impl_(std::move(impl)), schema_(schema) {}

  // API for accessing the result.
  bool IsStatement() const { return this->impl_->IsStatement(); }
  bool IsUpdate() const { return this->impl_->IsUpdate(); }
  bool IsQuery() const { return this->impl_->IsQuery(); }

  // Only safe to use if IsStatement() returns true.
  bool Success() const { return this->impl_->Success(); }

  // Only safe to use if IsUpdate() returns true.
  int UpdateCount() const { return this->impl_->UpdateCount(); }

  // Only safe to use if IsQuery() returns true.
  dataflow::SchemaRef GetSchema() const { return this->schema_; }
  bool HasNext() { return this->impl_->HasNext(); }
  dataflow::Record FetchOne() { return this->impl_->FetchOne(this->schema_); }
  SqlResult::SqlResultIterator begin() {
    return SqlResult::SqlResultIterator{this->impl_.get(), this->schema_};
  }
  SqlResult::SqlResultIterator end() {
    return SqlResult::SqlResultIterator{nullptr, this->schema_};
  }
  std::vector<dataflow::Record> Vectorize() {
    std::vector<dataflow::Record> result;
    for (dataflow::Record &record : *this) {
      result.push_back(std::move(record));
    }
    return result;
  }

  // Internal API: do not use outside of pelton code.
  // Appends the provided SqlResult to this SqlResult, appeneded
  // result is moved and becomes empty after append.
  void Append(SqlResult &&other);
  void AppendDeduplicate(SqlResult &&other);
  void MakeInline();

 private:
  std::unique_ptr<SqlResult::SqlResultImpl> impl_;
  dataflow::SchemaRef schema_;
};

// A non-query result.
class StatementResult : public SqlResult::SqlResultImpl {
 public:
  explicit StatementResult(bool success) : success_(success) {}

  bool IsStatement() const override { return true; }
  bool Success() const override { return this->success_; }

 private:
  bool success_;
};
class UpdateResult : public SqlResult::SqlResultImpl {
 public:
  explicit UpdateResult(int row_count) : row_count_(row_count) {}

  bool IsUpdate() const override { return true; }
  int UpdateCount() const override { return this->row_count_; }

  void IncrementCount(int count) {
    if (this->row_count_ >= 0) {
      if (count < 0) {
        this->row_count_ = count;
      } else {
        this->row_count_ += count;
      }
    }
  }

 private:
  int row_count_;
};

// Wrapper around an unmodified sql::ResultSet.
class MySqlResult : public SqlResult::SqlResultImpl {
 public:
  explicit MySqlResult(std::unique_ptr<sql::ResultSet> &&result)
      : result_(std::move(result)) {}

  bool IsQuery() const override;
  bool HasNext() override;
  dataflow::Record FetchOne(const dataflow::SchemaRef &schema) override;

 private:
  std::unique_ptr<sql::ResultSet> result_;
};

// Wrapper around a sql::ResultSet that is augmented with an additional value at
// some column.
class AugmentedSqlResult : public SqlResult::SqlResultImpl {
 public:
  AugmentedSqlResult(std::unique_ptr<sql::ResultSet> &&result, size_t aug_index,
                     const std::string &aug_value)
      : result_(std::move(result)),
        aug_index_(aug_index),
        aug_value_(aug_value) {}

  bool IsQuery() const override;
  bool HasNext() override;
  dataflow::Record FetchOne(const dataflow::SchemaRef &schema) override;

 private:
  std::unique_ptr<sql::ResultSet> result_;
  size_t aug_index_;
  std::string aug_value_;
};

// A result set with inlined values.
class InlinedSqlResult : public SqlResult::SqlResultImpl {
 public:
  explicit InlinedSqlResult(std::vector<dataflow::Record> &&records)
      : records_(std::move(records)), index_(0) {}

  bool IsQuery() const override;
  bool HasNext() override;
  dataflow::Record FetchOne(const dataflow::SchemaRef &schema) override;
  bool IsInlined() const override;

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
