#ifndef PELTON_SQL_RESULT_ABSTRACT_RESULT_H_
#define PELTON_SQL_RESULT_ABSTRACT_RESULT_H_

#include <memory>

#include "pelton/sql/result/resultset.h"

namespace pelton {
namespace sql {
namespace _result {

// Actual underlying implementation depends on whether the result
// corresponds to a SELECT query, INSERT/UPDATE/DELETE, or CREATE.
class AbstractSqlResultImpl {
 public:
  virtual ~AbstractSqlResultImpl() = default;

  virtual bool IsStatement() const;
  virtual bool IsUpdate() const;
  virtual bool IsQuery() const;

  // Statement API.
  virtual bool Success() const;

  // Update API.
  virtual int UpdateCount() const;

  // Query API.
  virtual bool HasResultSet();
  virtual std::unique_ptr<SqlResultSet> NextResultSet();
  virtual void AddResultSet(std::unique_ptr<SqlResultSet> &&result_set);

  // Appending API.
  virtual void Append(std::unique_ptr<AbstractSqlResultImpl> &&other,
                      bool deduplicate);

 protected:
  void AppendTypeCheck(AbstractSqlResultImpl *o) const;
};

}  // namespace _result
}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_RESULT_ABSTRACT_RESULT_H_
