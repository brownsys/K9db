#include "pelton/mysql/result.h"

namespace pelton {
namespace mysql {

// SqlResult.
void SqlResult::Append(SqlResult &&other) {
  if (other.hasData()) {
    if (!this->hasData()) {
      this->impl_ = std::move(other.impl_);
      return;
    }

    this->MakeInline();
    InlinedSqlResult *ptr =
        reinterpret_cast<InlinedSqlResult *>(this->impl_.get());
    size_t colnum = other.getColumnCount();
    while (other.count() > 0) {
      Row row = other.fetchOne();
      ptr->AddRow(InlinedRow(row, colnum));
    }
  }
}

void SqlResult::AppendDeduplicate(SqlResult &&other) {
  if (other.hasData()) {
    if (!this->hasData()) {
      this->impl_ = std::move(other.impl_);
      return;
    }

    this->MakeInline();
    InlinedSqlResult *ptr =
        reinterpret_cast<InlinedSqlResult *>(this->impl_.get());
    size_t colnum = other.getColumnCount();
    // Only add unduplicated rows.
    std::unordered_set<std::string> row_set = ptr->RowSet();
    while (other.count() > 0) {
      Row row = other.fetchOne();
      if (row_set.count(row.StringRepr(colnum)) == 0) {
        ptr->AddRow(InlinedRow(row, colnum));
      }
    }
  }
}

void SqlResult::MakeInline() {
  if (this->impl_->isInlined()) {
    return;
  }

  std::vector<InlinedRow> rows;
  std::vector<Column> cols;
  size_t colnum = this->getColumnCount();
  for (size_t i = 0; i < colnum; i++) {
    cols.push_back(this->getColumn(i));
  }
  while (this->count() > 0) {
    Row row = this->fetchOne();
    rows.emplace_back(row, colnum);
  }

  this->impl_ =
      std::make_unique<InlinedSqlResult>(std::move(rows), std::move(cols));
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

// InlinedSqlResult.
InlinedSqlResult::InlinedSqlResult(std::vector<InlinedRow> &&rows,
                                   std::vector<Column> &&cols)
    : rows_(), cols_(std::move(cols)), index_(0) {
  rows_.reserve(rows.size());
  for (size_t i = 0; i < rows.size(); i++) {
    rows_.emplace_back(std::make_unique<InlinedRow>(std::move(rows.at(i))));
  }
}

bool InlinedSqlResult::hasData() const { return true; }
uint64_t InlinedSqlResult::getAutoIncrementValue() { return 0; }

size_t InlinedSqlResult::getColumnCount() const { return this->cols_.size(); }
Column InlinedSqlResult::getColumn(size_t i) const { return this->cols_.at(i); }

Row InlinedSqlResult::fetchOne() {
  return std::move(this->rows_.at(this->index_++));
}
size_t InlinedSqlResult::count() { return this->rows_.size() - this->index_; }
bool InlinedSqlResult::isInlined() const { return true; }

std::unordered_set<std::string> InlinedSqlResult::RowSet() const {
  size_t colnum = this->getColumnCount();
  std::unordered_set<std::string> result;
  for (const auto &row : this->rows_) {
    result.insert(row.StringRepr(colnum));
  }
  return result;
}
void InlinedSqlResult::AddRow(InlinedRow &&row) {
  this->rows_.emplace_back(std::make_unique<InlinedRow>(std::move(row)));
}

}  // namespace mysql
}  // namespace pelton
