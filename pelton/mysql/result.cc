#include "pelton/mysql/result.h"

namespace pelton {
namespace mysql {

namespace {

std::unique_ptr<InlinedSqlResult> InlineSqlResult(SqlResult *input) {
  std::vector<Row> rows;
  std::vector<Column> cols;
  size_t colnum = input->getColumnCount();
  for (size_t i = 0; i < colnum; i++) {
    cols.push_back(input->getColumn(i));
  }
  while (input->count() > 0) {
    Row row = input->fetchOne();
    rows.push_back(std::move(row));
  }

  return std::make_unique<InlinedSqlResult>(std::move(rows), std::move(cols));
}

}  // namespace

// SqlResult.
void SqlResult::Append(SqlResult &&other) {
  if (other.impl_) {
    if (!this->impl_) {
      this->impl_ = std::move(other.impl_);
      return;
    }

    if (this->impl_->isNested()) {
      reinterpret_cast<NestedSqlResult *>(this->impl_.get())
          ->Append(std::move(other.impl_));
    } else if (other.impl_->isNested()) {
      reinterpret_cast<NestedSqlResult *>(other.impl_.get())
          ->Prepend(std::move(this->impl_));
      this->impl_ = std::move(other.impl_);
    } else {
      std::unique_ptr<NestedSqlResult> tmp =
          std::make_unique<NestedSqlResult>();
      tmp->Append(std::move(this->impl_));
      tmp->Append(std::move(other.impl_));
      this->impl_ = std::move(tmp);
    }
  }
}

void SqlResult::AppendDeduplicate(SqlResult &&other) {
  if (other.impl_) {
    if (!this->impl_) {
      this->impl_ = std::move(other.impl_);
      return;
    }

    // Turn rows to inlined rows.
    if (!this->impl_->isInlined()) {
      this->impl_ = InlineSqlResult(this);
    }

    // Only add unduplicated rows.
    size_t colnum = other.getColumnCount();
    InlinedSqlResult *ptr =
        reinterpret_cast<InlinedSqlResult *>(this->impl_.get());
    std::unordered_set<std::string> row_set = ptr->RowSet();
    while (other.count() > 0) {
      Row row = other.fetchOne();
      if (row_set.count(row.StringRepr(colnum)) == 0) {
        ptr->AddRow(std::move(row));
      }
    }
  }
}

// MySqlResult.
bool MySqlResult::hasData() const { return this->result_.hasData(); }
uint64_t MySqlResult::getAutoIncrementValue() {
  return this->result_.getAutoIncrementValue();
}

size_t MySqlResult::getColumnCount() const {
  return this->result_.getColumnCount();
}
Column MySqlResult::getColumn(size_t i) const {
  return Column(this->result_.getColumn(i));
}

Row MySqlResult::fetchOne() {
  return Row(std::make_unique<MySqlRow>(this->result_.fetchOne()));
}
size_t MySqlResult::count() { return this->result_.count(); }
bool MySqlResult::isInlined() const { return false; }
bool MySqlResult::isNested() const { return false; }

// AugmentedSqlResult.
bool AugmentedSqlResult::hasData() const { return this->result_.hasData(); }
uint64_t AugmentedSqlResult::getAutoIncrementValue() {
  return this->result_.getAutoIncrementValue();
}

size_t AugmentedSqlResult::getColumnCount() const {
  return this->result_.getColumnCount() + 1;
}
Column AugmentedSqlResult::getColumn(size_t i) const {
  if (i == this->aug_index_) {
    return this->aug_column_;
  } else if (i < this->aug_index_) {
    return Column(this->result_.getColumn(i));
  } else {
    return Column(this->result_.getColumn(i - 1));
  }
}

Row AugmentedSqlResult::fetchOne() {
  return Row(std::make_unique<AugmentedRow>(
      this->result_.fetchOne(), this->aug_index_, this->aug_value_));
}
size_t AugmentedSqlResult::count() { return this->result_.count(); }
bool AugmentedSqlResult::isInlined() const { return false; }
bool AugmentedSqlResult::isNested() const { return false; }

// InlinedSqlResult.
bool InlinedSqlResult::hasData() const { return true; }
uint64_t InlinedSqlResult::getAutoIncrementValue() { return 0; }

size_t InlinedSqlResult::getColumnCount() const { return this->cols_.size(); }
Column InlinedSqlResult::getColumn(size_t i) const { return this->cols_.at(i); }

Row InlinedSqlResult::fetchOne() {
  return std::move(this->rows_.at(this->index_++));
}
size_t InlinedSqlResult::count() { return this->rows_.size() - this->index_; }
bool InlinedSqlResult::isInlined() const { return true; }
bool InlinedSqlResult::isNested() const { return false; }

std::unordered_set<std::string> InlinedSqlResult::RowSet() const {
  size_t colnum = this->getColumnCount();
  std::unordered_set<std::string> result;
  for (const auto &row : this->rows_) {
    result.insert(row.StringRepr(colnum));
  }
  return result;
}
void InlinedSqlResult::AddRow(Row &&row) {
  this->rows_.push_back(std::move(row));
}

// NestedSqlResult.
bool NestedSqlResult::hasData() const {
  for (const auto &ptr : this->results_) {
    if (ptr->hasData()) {
      return true;
    }
  }
  return false;
}
uint64_t NestedSqlResult::getAutoIncrementValue() {
  return this->results_.front()->getAutoIncrementValue();
}

size_t NestedSqlResult::getColumnCount() const {
  return this->results_.front()->getColumnCount();
}
Column NestedSqlResult::getColumn(size_t i) const {
  return this->results_.front()->getColumn(i);
}

Row NestedSqlResult::fetchOne() {
  this->count_--;
  while (this->results_.front()->count() == 0) {
    this->results_.pop_front();
  }
  return this->results_.front()->fetchOne();
}
size_t NestedSqlResult::count() { return this->count_; }
bool NestedSqlResult::isInlined() const { return false; }
bool NestedSqlResult::isNested() const { return true; }

// Appending and Prepending.
void NestedSqlResult::Append(std::unique_ptr<SqlResultImpl> &&other) {
  this->count_ += other->count();
  if (other->isNested()) {
    this->results_.splice(
        this->results_.end(),
        reinterpret_cast<NestedSqlResult *>(other.get())->results_);
  } else {
    this->results_.push_back(std::move(other));
  }
}
void NestedSqlResult::Prepend(std::unique_ptr<SqlResultImpl> &&other) {
  this->count_ += other->count();
  if (other->isNested()) {
    this->results_.splice(
        this->results_.begin(),
        reinterpret_cast<NestedSqlResult *>(other.get())->results_);
  } else {
    this->results_.push_front(std::move(other));
  }
}

}  // namespace mysql
}  // namespace pelton
