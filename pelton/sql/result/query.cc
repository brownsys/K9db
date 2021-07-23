#include "pelton/sql/result/query.h"

#include <utility>

#include "glog/logging.h"

namespace pelton {
namespace sql {
namespace _result {

// Result type.
bool QuerySqlResult::IsQuery() const { return true; }

// Query API.
bool QuerySqlResult::HasResultSet() {
  return this->index_ < this->results_.size();
}

std::unique_ptr<SqlResultSet> QuerySqlResult::NextResultSet() {
  return std::move(this->results_.at(this->index_++));
}

void QuerySqlResult::AddResultSet(std::unique_ptr<SqlResultSet> &&result_set) {
  this->results_.push_back(std::move(result_set));
}

// Appending.
void QuerySqlResult::Append(std::unique_ptr<AbstractSqlResultImpl> &&other,
                            bool deduplicate) {
  // Make sure other is also an UpdateSqlResult.
  this->AppendTypeCheck(other.get());
  QuerySqlResult *o = static_cast<QuerySqlResult *>(other.get());

  // Assert that this result set and the appended one are compatible.
  CHECK(this->results_.size() == 1) << "Can only append into 1 resultset";
  CHECK(o->results_.size() == 1) << "Can only append by 1 resultset";

  // Append consuming other.
  this->results_.at(0)->Append(std::move(o->results_.at(0)), deduplicate);
}

}  // namespace _result
}  // namespace sql
}  // namespace pelton
