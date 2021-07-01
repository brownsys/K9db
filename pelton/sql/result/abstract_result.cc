#include "pelton/sql/result/abstract_result.h"

#include <utility>

#include "glog/logging.h"

namespace pelton {
namespace sql {
namespace _result {

// Result type.
bool AbstractSqlResultImpl::IsStatement() const { return false; }
bool AbstractSqlResultImpl::IsUpdate() const { return false; }
bool AbstractSqlResultImpl::IsQuery() const { return false; }

// Statement API.
bool AbstractSqlResultImpl::Success() const {
  LOG(FATAL) << ".Success() called on non-statement SqlResult";
}

// Update API.
int AbstractSqlResultImpl::UpdateCount() const {
  LOG(FATAL) << ".UpdateCount() called on non-update SqlResult";
}

// Query API.
bool AbstractSqlResultImpl::HasResultSet() {
  LOG(FATAL) << ".HasResultSet() called on non-query SqlResult";
}
SqlResultSet AbstractSqlResultImpl::NextResultSet() {
  LOG(FATAL) << ".NextResultSet() called on non-query SqlResult";
}
void AbstractSqlResultImpl::AddResultSet(SqlResultSet &&result_set) {
  LOG(FATAL) << ".AddResultSet() called on non-query SqlResult";
}

// Appending API.
void AbstractSqlResultImpl::Append(
    std::unique_ptr<AbstractSqlResultImpl> &&other, bool deduplicate) {
  LOG(FATAL) << ".Append() called on unsupported SqlResult";
}

void AbstractSqlResultImpl::AppendTypeCheck(AbstractSqlResultImpl *o) const {
  if (this->IsStatement()) {
    CHECK(o->IsStatement()) << "Bad append to statement SqlResult";
  } else if (this->IsUpdate()) {
    CHECK(o->IsUpdate()) << "Bad append to update SqlResult";
  } else if (this->IsQuery()) {
    CHECK(o->IsQuery()) << "Bad append to query SqlResult";
  }
}

}  // namespace _result
}  // namespace sql
}  // namespace pelton
