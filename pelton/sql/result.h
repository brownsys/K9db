#ifndef PELTON_SQL_RESULT_H_
#define PELTON_SQL_RESULT_H_

#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/connection.h"

namespace pelton {
namespace sql {

// An SqlResultSet represents the content of a single table.
// An SqlResult may include multiple results when it reads from multiple tables!
class SqlResultSet {
 public:
  // Constructors.
  explicit SqlResultSet(const dataflow::SchemaRef &schema) : schema_(schema) {}

  SqlResultSet(const dataflow::SchemaRef &schema,
               RecordKeyVecs &&records)
      : schema_(schema), records_(std::move(records)) {}

  // Adding additional results to this set.
  void Append(SqlResultSet &&other, bool deduplicate);

  // Query API.
  const dataflow::SchemaRef &schema() const { return this->schema_; }
  std::vector<dataflow::Record> &&Vec() {
    return std::move(this->records_.records);
  }

 private:
  dataflow::SchemaRef schema_;
  RecordKeyVecs records_;
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
  inline bool Success() const { return this->status_; }

  // Only safe to use if IsUpdate() returns true.
  inline int UpdateCount() const { return this->status_; }

  // Only safe to use if IsQuery() returns true.
  const std::vector<SqlResultSet> &ResultSets() const { return this->sets_; }
  std::vector<SqlResultSet> &ResultSets() { return this->sets_; }

  // Internal API: do not use outside of pelton code.
  // Appends the provided SqlResult to this SqlResult, appeneded
  // result is moved and becomes empty after append.
  void Append(SqlResult &&other, bool deduplicate);
  inline void AddResultSet(SqlResultSet &&resultset) {
    this->sets_.push_back(std::move(resultset));
  }

 private:
  Type type_;
  int status_;
  std::vector<SqlResultSet> sets_;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_RESULT_H_
