// Create table statement and sub expressions.
#include "pelton/sqlast/ast_schema.h"

#include <cassert>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/match.h"

namespace pelton {
namespace sqlast {

// ColumnConstraint.
const std::string &ColumnConstraint::foreign_table() const {
  assert(this->type_ == Type::FOREIGN_KEY);
  return this->foreign_table_;
}
std::string &ColumnConstraint::foreign_table() {
  assert(this->type_ == Type::FOREIGN_KEY);
  return this->foreign_table_;
}
const std::string &ColumnConstraint::foreign_column() const {
  assert(this->type_ == Type::FOREIGN_KEY);
  return this->foreign_column_;
}
std::string &ColumnConstraint::foreign_column() {
  assert(this->type_ == Type::FOREIGN_KEY);
  return this->foreign_column_;
}

// ColumnDefinition
ColumnDefinition::Type ColumnDefinition::StringToType(
    const std::string &column_type) {
  if (absl::EqualsIgnoreCase(column_type, "int")) {
    return Type::INT;
  } else if (absl::StartsWithIgnoreCase(column_type, "varchar") ||
             absl::EqualsIgnoreCase(column_type, "string")) {
    return Type::TEXT;
  } else {
    throw "Unsupported datatype!";
  }
}

std::string ColumnDefinition::TypeToString(ColumnDefinition::Type type) {
  switch (type) {
    case Type::INT:
      return "int";
    case Type::TEXT:
      return "varchar";
    default:
      throw "Unsupported datatype!";
  }
}

const std::string &ColumnDefinition::column_name() const {
  return this->column_name_;
}
std::string &ColumnDefinition::column_name() { return this->column_name_; }
ColumnDefinition::Type ColumnDefinition::column_type() const {
  return this->column_type_;
}
ColumnDefinition::Type &ColumnDefinition::column_type() {
  return this->column_type_;
}

void ColumnDefinition::AddConstraint(const ColumnConstraint &constraint) {
  this->constraints_.push_back(constraint);
}

const std::vector<ColumnConstraint> &ColumnDefinition::GetConstraints() const {
  return this->constraints_;
}
std::vector<ColumnConstraint> &ColumnDefinition::MutableConstraints() {
  return this->constraints_;
}

const ColumnConstraint &ColumnDefinition::GetConstraint(size_t index) const {
  return this->constraints_.at(index);
}
ColumnConstraint &ColumnDefinition::MutableConstraint(size_t index) {
  return this->constraints_.at(index);
}

void ColumnDefinition::RemoveConstraint(size_t index) {
  this->constraints_.erase(this->constraints_.begin() + index);
}
void ColumnDefinition::RemoveConstraint(ColumnConstraint::Type type) {
  for (size_t i = 0; i < this->constraints_.size(); i++) {
    if (this->constraints_[i].type() == type) {
      this->RemoveConstraint(i);
      i--;
    }
  }
}

bool ColumnDefinition::HasConstraint(ColumnConstraint::Type type) const {
  for (const auto &constraint : this->constraints_) {
    if (constraint.type() == type) {
      return true;
    }
  }
  return false;
}

// CreateTable
const std::string &CreateTable::table_name() const { return this->table_name_; }
std::string &CreateTable::table_name() { return this->table_name_; }

void CreateTable::AddColumn(const std::string &column_name,
                            const ColumnDefinition &def) {
  this->columns_map_.insert({column_name, this->columns_.size()});
  this->columns_.push_back(def);
}

const std::vector<ColumnDefinition> &CreateTable::GetColumns() const {
  return this->columns_;
}
const ColumnDefinition &CreateTable::GetColumn(
    const std::string &column_name) const {
  size_t column_index = this->columns_map_.at(column_name);
  return this->columns_.at(column_index);
}
ColumnDefinition &CreateTable::MutableColumn(const std::string &column_name) {
  size_t column_index = this->columns_map_.at(column_name);
  return this->columns_.at(column_index);
}

bool CreateTable::HasColumn(const std::string &column_name) {
  return this->columns_map_.count(column_name) == 1;
}
size_t CreateTable::ColumnIndex(const std::string &column_name) {
  return this->columns_map_.at(column_name);
}
void CreateTable::RemoveColumn(const std::string &column_name) {
  size_t column_index = this->columns_map_.at(column_name);
  this->columns_.erase(this->columns_.begin() + column_index);
  this->columns_map_.erase(column_name);
  for (size_t i = column_index; i < this->columns_.size(); i++) {
    this->columns_map_.at(this->columns_.at(i).column_name()) = i;
  }
}

}  // namespace sqlast
}  // namespace pelton
