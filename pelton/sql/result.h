#ifndef PELTON_SQL_RESULT_H_
#define PELTON_SQL_RESULT_H_

#include <memory>
#include <utility>

#include "pelton/dataflow/schema.h"
#include "pelton/sql/result/abstract_result.h"
#include "pelton/sql/result/resultset.h"

namespace pelton {
namespace sql {

// SqlResultSet should be exposed to client code.
using SqlResultSet = _result::SqlResultSet;

// Our version of an (abstract) SqlResult.
// This class provides functions to iterate through the records of a result
// in addition to the schema.
class SqlResult {
 public:
  // Constructors.
  // Generic empty SqlResult without any underlying implementation or specified
  // type. Gets filled later with actual data by calling .Append(...)
  SqlResult();
  // For results of DDL (e.g. CREATE TABLE).
  explicit SqlResult(bool status);
  // For results of DML (e.g. INSERT/UPDATE/DELETE).
  explicit SqlResult(int row_count);
  // For results of SELECT, the result_set object is actually a lazy wrapper
  // around plain SQL commands and some schema and other metadata. The actual
  // records/rows are produced as they are retrieved from this wrapper.
  explicit SqlResult(std::unique_ptr<SqlResultSet> &&result_set);
  explicit SqlResult(const dataflow::SchemaRef &schema);  // For empty SELECTs.

  // API for accessing the result.
  inline bool IsStatement() const {
    return this->impl_ && this->impl_->IsStatement();
  }
  inline bool IsUpdate() const {
    return this->impl_ && this->impl_->IsUpdate();
  }
  inline bool IsQuery() const { return this->impl_ && this->impl_->IsQuery(); }

  // Only safe to use if IsStatement() returns true.
  inline bool Success() const { return this->impl_->Success(); }

  // Only safe to use if IsUpdate() returns true.
  inline int UpdateCount() const { return this->impl_->UpdateCount(); }

  // Only safe to use if IsQuery() returns true.
  inline bool HasResultSet() { return this->impl_->HasResultSet(); }
  inline std::unique_ptr<SqlResultSet> NextResultSet() {
    return this->impl_->NextResultSet();
  }

  // Internal API: do not use outside of pelton code.
  // Appends the provided SqlResult to this SqlResult, appeneded
  // result is moved and becomes empty after append.
  inline void Append(SqlResult &&other, bool deduplicate = false) {
    if (this->impl_ == nullptr) {
      this->impl_ = std::move(other.impl_);
    } else {
      this->impl_->Append(std::move(other.impl_), deduplicate);
    }
  }
  inline void AddResultSet(std::unique_ptr<SqlResultSet> &&result_set) {
    if (this->impl_ == nullptr) {
      this->Append(SqlResult(std::move(result_set)));
    } else {
      this->impl_->AddResultSet(std::move(result_set));
    }
  }

 private:
  std::unique_ptr<_result::AbstractSqlResultImpl> impl_;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_RESULT_H_
