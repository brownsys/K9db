#include "pelton/sql/result.h"

#include "pelton/sql/result/query.h"
#include "pelton/sql/result/statement.h"
#include "pelton/sql/result/update.h"

namespace pelton {
namespace sql {

using StatementSqlResult = _result::StatementSqlResult;
using UpdateSqlResult = _result::UpdateSqlResult;
using QuerySqlResult = _result::QuerySqlResult;
using SqlInlineResultSet = _result::SqlInlineResultSet;

// Constructors.

// Empty generic SqlResult with no specified type.
SqlResult::SqlResult() : impl_(nullptr) {}

// For results of DDL (e.g. CREATE TABLE).
SqlResult::SqlResult(bool status)
    : impl_(std::make_unique<StatementSqlResult>(status)) {}

// For results of DML (e.g. INSERT/UPDATE/DELETE).
SqlResult::SqlResult(int row_count)
    : impl_(std::make_unique<UpdateSqlResult>(row_count)) {}

// For results of SELECT, the result_set object is actually a lazy wrapper
// around plain SQL commands and some schema and other metadata. The actual
// records/rows are produced as they are retrieved from this wrapper.
SqlResult::SqlResult(std::unique_ptr<SqlResultSet> &&result_set)
    : impl_(std::make_unique<QuerySqlResult>()) {
  this->impl_->AddResultSet(std::move(result_set));
}

// For empty SELECTs.
SqlResult::SqlResult(const dataflow::SchemaRef &schema)
    : impl_(std::make_unique<QuerySqlResult>()) {
  this->impl_->AddResultSet(std::make_unique<SqlInlineResultSet>(schema));
}

}  // namespace sql
}  // namespace pelton
