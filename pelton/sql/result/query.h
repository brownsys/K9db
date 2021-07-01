#ifndef PELTON_SQL_RESULT_QUERY_H_
#define PELTON_SQL_RESULT_QUERY_H_

#include <memory>
#include <vector>

#include "pelton/sql/result/abstract_result.h"
#include "pelton/sql/result/resultset.h"

namespace pelton {
namespace sql {
namespace _result {

class QuerySqlResult : public AbstractSqlResultImpl {
 public:
  QuerySqlResult() : index_(0), results_() {}

  // Result type.
  bool IsQuery() const override;

  // Query API.
  bool HasResultSet() override;
  std::unique_ptr<SqlResultSet> NextResultSet() override;
  void AddResultSet(std::unique_ptr<SqlResultSet> &&result_set) override;

  // Appending.
  void Append(std::unique_ptr<AbstractSqlResultImpl> &&other,
              bool deduplicate) override;

 private:
  size_t index_;
  std::vector<std::unique_ptr<SqlResultSet>> results_;
};

}  // namespace _result
}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_RESULT_QUERY_H_
