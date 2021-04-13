// Our version of mysqlx API types.
#include "pelton/types.h"

namespace pelton {

// Column.
Column::Column(const mysqlx::Column &col)
    : type_(col.getType()), name_(col.getColumnName()) {}

Column::Column(mysqlx::Type type, const std::string &name)
    : type_(type), name_(name) {}

mysqlx::Type Column::getType() const { return this->type_; }

const std::string &Column::getColumnName() const { return this->name_; }

// Row.
Row::Row(mysqlx::Row &&row, const std::vector<size_t> &aug_indices,
         const std::vector<mysqlx::Value> &aug_values)
    : row_(std::move(row)),
      aug_indices_(aug_indices),
      aug_values_(aug_values) {}

const mysqlx::Value &Row::get(size_t pos) const { return (*this)[pos]; }

const mysqlx::Value &Row::operator[](size_t i) const {
  size_t index = i;
  for (size_t c = 0; c < this->aug_indices_.size(); c++) {
    size_t a = this->aug_indices_.at(c);
    if (a == i) {
      return this->aug_values_.at(c);
    }
    if (i < a) {
      break;
    }
    if (i > a) {
      index--;
    }
  }

  return this->row_[index];
}

bool Row::isNull() const {
  return this->aug_values_.size() == 0 && this->row_.isNull();
}

// SqlResult.
SqlResult::SqlResult(std::vector<size_t> &&aug_indices,
                     std::vector<Column> &&aug_cols)
    : result_index_(0),
      count_(0),
      aug_indices_(std::move(aug_indices)),
      aug_columns_(std::move(aug_cols)) {}

void SqlResult::AddResult(mysqlx::SqlResult &&result) {
  this->count_ += result.count();
  this->results_.push_back(std::move(result));
  this->aug_values_.push_back({});
}

void SqlResult::Consume(SqlResult *other) {
  this->count_ += other->count_;
  if (this->aug_indices_.size() == 0 && other->aug_indices_.size() > 0) {
    this->aug_indices_ = std::move(other->aug_indices_);
    this->aug_columns_ = std::move(other->aug_columns_);
  }
  for (size_t i = 0; i < other->results_.size(); i++) {
    this->results_.push_back(std::move(other->results_.at(i)));
  }
  for (size_t i = 0; i < other->aug_values_.size(); i++) {
    this->aug_values_.push_back(std::move(other->aug_values_.at(i)));
  }
}

bool SqlResult::hasData() const {
  if (this->results_.size() > 0) {
    for (size_t i = this->result_index_; i < this->results_.size(); i++) {
      if (this->results_.at(i).hasData()) {
        return true;
      }
    }
    return false;
  }
  return this->aug_columns_.size() > 0;
}

uint64_t SqlResult::getAutoIncrementValue() {
  return this->results_.at(0).getAutoIncrementValue();
}

size_t SqlResult::getColumnCount() const {
  if (this->results_.size() > 0) {
    return this->results_.at(0).getColumnCount() + this->aug_columns_.size();
  }

  return this->aug_columns_.size();
}

Column SqlResult::getColumn(size_t i) const {
  size_t index = i;
  for (size_t c = 0; c < this->aug_indices_.size(); c++) {
    size_t a = this->aug_indices_.at(c);
    if (a == i) {
      return this->aug_columns_.at(c);
    }
    if (i < a) {
      break;
    }
    if (i > a) {
      index--;
    }
  }

  return Column(this->results_.at(0).getColumn(index));
}

Row SqlResult::fetchOne() {
  this->count_--;
  if (this->results_.size() > 0) {
    // MySql result with potential augmentation.
    while (!this->results_.at(this->result_index_).hasData() ||
           this->results_.at(this->result_index_).count() == 0) {
      this->result_index_++;
    }
    return Row(results_.at(this->result_index_).fetchOne(), this->aug_indices_,
               this->aug_values_.at(this->result_index_));
  } else {
    // No MySql result, only augmented things!
    return Row(this->aug_indices_, this->aug_values_.at(this->result_index_++));
  }
}

size_t SqlResult::count() const { return this->count_; }

}  // namespace pelton
