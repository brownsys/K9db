#ifndef PELTON_SQL_RESULT_STATEMENT_H_
#define PELTON_SQL_RESULT_STATEMENT_H_

#include <memory>

#include "pelton/sql/result/abstract_result.h"

namespace pelton {
namespace sql {
namespace _result {

class StatementSqlResult : public AbstractSqlResultImpl {
 public:
  explicit StatementSqlResult(bool status) : status_(status) {}

  bool IsStatement() const override { return true; }
  bool Success() const override { return this->status_; }

  void Append(std::unique_ptr<AbstractSqlResultImpl> &&other,
              bool deduplicate) override {
    AbstractSqlResultImpl *o = other.get();
    // Make sure other is also an StatementSqlResult.
    this->AppendTypeCheck(o);
    bool other_status = static_cast<StatementSqlResult *>(o)->status_;
    this->status_ = this->status_ && other_status;
  }

 private:
  bool status_;
};

}  // namespace _result
}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_RESULT_STATEMENT_H_
