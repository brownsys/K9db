#ifndef PELTON_SQL_RESULT_UPDATE_H_
#define PELTON_SQL_RESULT_UPDATE_H_

#include <memory>

#include "pelton/sql/result/abstract_result.h"

namespace pelton {
namespace sql {
namespace _result {

class UpdateSqlResult : public AbstractSqlResultImpl {
 public:
  explicit UpdateSqlResult(int row_count) : row_count_(row_count) {}

  bool IsUpdate() const override { return true; }
  int UpdateCount() const override { return this->row_count_; }

  void Append(std::unique_ptr<AbstractSqlResultImpl> &&other,
              bool deduplicate) override {
    AbstractSqlResultImpl *o = other.get();
    // Make sure other is also an UpdateSqlResult.
    this->AppendTypeCheck(o);
    int other_count = static_cast<UpdateSqlResult *>(o)->row_count_;
    if (other_count < 0) {
      this->row_count_ = other_count;  // Error code.
    } else if (this->row_count_ > -1) {
      this->row_count_ += other_count;  // Both results were successful.
    }
  }

 private:
  int row_count_;
};

}  // namespace _result
}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_RESULT_UPDATE_H_
