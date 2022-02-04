#ifndef PELTON_SQL_RESULT_H_
#define PELTON_SQL_RESULT_H_

#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"

namespace pelton {
namespace sql {

// An SqlResultSet represents the content of a single table.
// An SqlResult may include multiple results when it reads from multiple tables!
class SqlResultSet {
 public:
  // Constructors.
  explicit SqlResultSet(const dataflow::SchemaRef &schema) : schema_(schema) {}

  SqlResultSet(const dataflow::SchemaRef &schema,
               std::vector<dataflow::Record> &&records)
      : schema_(schema), records_(std::move(records)), keys_() {}

  SqlResultSet(const dataflow::SchemaRef &schema,
               std::vector<dataflow::Record> &&records,
               std::vector<std::string> &&keys)
      : schema_(schema), records_(std::move(records)), keys_(std::move(keys)) {}

  // Adding additional results to this set.
  void Append(SqlResultSet &&other, bool deduplicate);

  // Query API.
  const dataflow::SchemaRef &schema() const { return this->schema_; }
  std::vector<dataflow::Record> &&Vec() { return std::move(this->records_); }
  inline size_t size() const { return this->records_.size(); }

  inline bool empty() const { return this->records_.empty(); }

 private:
  dataflow::SchemaRef schema_;
  std::vector<dataflow::Record> records_;
  std::vector<std::string> keys_;
};

// Our version of an SqlResult.
// Might store a boolean or int status, or several ResultSets
class SqlResult {
 private:
  enum class Type { STATEMENT, UPDATE, QUERY };

 public:
  // Constructors.
  // For results of DDL (e.g. CREATE TABLE).
  // Success is true if and only if the DDL statement succeeded.
  explicit SqlResult(bool success) : type_(Type::STATEMENT), status_(success) {}
  // For results of DML (e.g. INSERT/UPDATE/DELETE).
  // row_count specifies how many rows were affected (-1 for failures).
  explicit SqlResult(int rows) : type_(Type::UPDATE), status_(rows) {}
  explicit SqlResult(size_t rows) : SqlResult(static_cast<int>(rows)) {}
  // For results of SELECT.
  explicit SqlResult(std::vector<SqlResultSet> &&v)
      : type_(Type::QUERY), sets_(std::move(v)) {}
  explicit SqlResult(SqlResultSet &&resultset) : type_(Type::QUERY) {
    this->sets_.push_back(std::move(resultset));
  }
  explicit SqlResult(const dataflow::SchemaRef &schema) : type_(Type::QUERY) {
    this->sets_.emplace_back(schema);
  }

  // API for accessing the result.
  inline bool IsStatement() const { return this->type_ == Type::STATEMENT; }
  inline bool IsUpdate() const { return this->type_ == Type::UPDATE; }
  inline bool IsQuery() const { return this->type_ == Type::QUERY; }

  // Only safe to use if IsStatement() returns true.
  inline bool Success() const {
    if (!this->IsStatement())
      DLOG(FATAL) << "Success() called on non-statement result (" << this->type_
                  << ")";
    return this->status_;
  }

  // Only safe to use if IsUpdate() returns true.
  inline int UpdateCount() const {
    if (!this->IsUpdate()) {
      DLOG(FATAL) << "UpdateCount() called on non-update result ("
                  << this->type_ << ")";
    }
    return this->status_;
  }

  // Only safe to use if IsQuery() returns true.
  const std::vector<SqlResultSet> &ResultSets() const {
    if (!this->IsQuery()) {
      DLOG(FATAL) << "ResultSets() called on non-query result";
    }
    return this->sets_;
  }
  std::vector<SqlResultSet> &ResultSets() {
    if (!this->IsQuery()) {
      DLOG(FATAL) << "ResultSets() called on non-query result";
    }
    return this->sets_;
  }

  // NOTE(justus): This function is only safe to call if `IsQuery()` returns
  // true.
  bool empty() const;

  // Internal API: do not use outside of pelton code.
  // Appends the provided SqlResult to this SqlResult, appeneded
  // result is moved and becomes empty after append.
  void Append(SqlResult &&other, bool deduplicate);
  inline void AddResultSet(SqlResultSet &&resultset) {
    this->sets_.push_back(std::move(resultset));
  }

  friend std::ostream &operator<<(std::ostream &str,
                                  const SqlResult::Type &res);

 private:
  Type type_;
  int status_;
  std::vector<SqlResultSet> sets_;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_RESULT_H_
