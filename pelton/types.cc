// Our version of mysqlx API types.
#include "pelton/types.h"

namespace pelton {

// Column.
Column::Column(const mysqlx::Column &col)
    : type_(col.getType()), name_(col.getColumnName()) {}

Column::Column(mysqlx::Type type, const std::string &name)
    : type_(type), name_(name) {}

mysqlx::Type Column::getType() const { return this->type_; }

const mysqlx::string &Column::getColumnName() const { return this->name_; }

// Row.
Row::Row(mysqlx::Row &&row, int index, const mysqlx::Value &value)
    : row_(std::move(row)), augmented_index_(index), augmented_value_(value) {}

const mysqlx::Value &Row::get(size_t pos) {
  if (this->augmented_index_ < 0 ||
      pos < static_cast<size_t>(this->augmented_index_)) {
    return this->row_.get(pos);
  } else if (pos > static_cast<size_t>(this->augmented_index_)) {
    return this->row_.get(pos - 1);
  } else {
    return this->augmented_value_;
  }
}

const mysqlx::Value &Row::operator[](size_t pos) const {
  if (this->augmented_index_ < 0 ||
      pos < static_cast<size_t>(this->augmented_index_)) {
    return this->row_[pos];
  } else if (pos > static_cast<size_t>(this->augmented_index_)) {
    return this->row_[pos - 1];
  } else {
    return this->augmented_value_;
  }
}

bool Row::isNull() const {
  return this->augmented_index_ == -1 && this->row_.isNull();
}

// SqlResult.
SqlResult::SqlResult()
    : result_index_(0), count_(0), augmented_index_(-1), augmented_column_() {}

SqlResult::SqlResult(int augmented_index, Column augmented_column)
    : result_index_(0),
      count_(0),
      augmented_index_(augmented_index),
      augmented_column_(augmented_column) {}

void SqlResult::AddResult(mysqlx::SqlResult &&result) {
  this->count_ += result.count();
  this->results_.push_back(std::move(result));
  this->augmented_values_.emplace_back(nullptr);
}

bool SqlResult::hasData() {
  while (this->result_index_ < this->results_.size()) {
    if (this->results_.at(this->result_index_).hasData()) {
      return true;
    }
    this->result_index_++;
  }
  return false;
}

uint64_t SqlResult::getAutoIncrementValue() {
  return this->results_.at(0).getAutoIncrementValue();
}

size_t SqlResult::getColumnCount() const {
  if (this->results_.size() == 0) {
    return 0;
  }
  size_t augmented = this->augmented_index_ > 1 ? 1 : 0;
  return this->results_.at(0).getColumnCount() + augmented;
}

Column SqlResult::getColumn(size_t i) const {
  if (this->augmented_index_ < 0 ||
      i < static_cast<size_t>(this->augmented_index_)) {
    return Column(this->results_.at(0).getColumn(i));
  } else if (i > static_cast<size_t>(this->augmented_index_)) {
    return Column(this->results_.at(0).getColumn(i - 1));
  } else {
    return this->augmented_column_;
  }
}

Row SqlResult::fetchOne() {
  while (!this->results_.at(this->result_index_).hasData()) {
    this->result_index_++;
  }
  return Row(results_.at(this->result_index_).fetchOne(),
             this->augmented_index_,
             this->augmented_values_.at(this->result_index_));
}

size_t SqlResult::count() const { return this->count_; }

}  // namespace pelton
