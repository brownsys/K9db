#include "pelton/sql/result.h"

namespace pelton {
namespace sql {

// Constructors.

// Empty generic SqlResult with no specified type.
SqlResult::SqlResult() : impl_(nullptr) {}

// For results of DDL (e.g. CREATE TABLE).
SqlResult::SqlResult(bool status) {}

// For results of DML (e.g. INSERT/UPDATE/DELETE).
SqlResult::SqlResult(int row_count) {}

// For results of SELECT, the result_set object is actually a lazy wrapper
// around plain SQL commands and some schema and other metadata. The actual
// records/rows are produced as they are retrieved from this wrapper.
SqlResult::SqlResult(SqlResultSet &&result_set) {}

}  // namespace sql
}  // namespace pelton
