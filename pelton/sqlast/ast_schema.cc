// Create table statement and sub expressions.
#include "pelton/sqlast/ast_schema.h"

#include <cassert>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "glog/logging.h"

namespace pelton {
namespace sqlast {

// ColumnConstraint.
std::string ColumnConstraint::TypeToString(Type type) {
  switch (type) {
    case Type::PRIMARY_KEY:
      return "PRIMARY KEY";
    case Type::NOT_NULL:
      return "NOT NULL";
    case Type::UNIQUE:
      return "UNIQUE";
    case Type::FOREIGN_KEY:
      return "FOREIGN KEY";
    default:
      LOG(FATAL) << "Unsupported constraint type!";
  }
}

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
  if (absl::EqualsIgnoreCase(column_type, "INT") ||
      absl::EqualsIgnoreCase(column_type, "INTEGER")) {
    return Type::INT;
  } else if (absl::StartsWithIgnoreCase(column_type, "VARCHAR") ||
             absl::EqualsIgnoreCase(column_type, "TEXT")) {
    return Type::TEXT;
  } else if (absl::StartsWithIgnoreCase(column_type, "DATETIME")) {
    return Type::DATETIME;
  } else {
    LOG(FATAL) << "Unsupported column type: " << column_type;
  }
}

std::string ColumnDefinition::TypeToString(ColumnDefinition::Type type) {
  switch (type) {
    case Type::INT:
      return "INT";
    case Type::DATETIME:
      return "DATETIME";
    case Type::TEXT:
      return "VARCHAR(100)";
    default:
      LOG(FATAL) << "Unsupported column type: " << type;
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

bool CreateTable::HasColumn(const std::string &column_name) const {
  return this->columns_map_.count(column_name) == 1;
}
size_t CreateTable::ColumnIndex(const std::string &column_name) const {
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

std::ostream &operator<<(std::ostream &os, const ColumnConstraint::Type &r) {
  os << ColumnConstraint::TypeToString(r);
  return os;
}

std::ostream &operator<<(std::ostream &os, const ColumnDefinition::Type &r) {
  switch (r) {
    case ColumnDefinition::Type::INT:
      os << "INT";
      break;
    case ColumnDefinition::Type::TEXT:
      os << "TEXT";
      break;
    case ColumnDefinition::Type::UINT:
      os << "UINT";
      break;
    case ColumnDefinition::Type::DATETIME:
      os << "DATETIME";
      break;
    default:
      LOG(FATAL) << "Unsupported column type: " << r;
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, const AbstractStatement::Type &t) {
  switch (t) {
    case AbstractStatement::Type::CREATE_TABLE:
      return os << "CREATE TABLE";
    case AbstractStatement::Type::CREATE_INDEX:
      return os << "CREATE INDEX";
    case AbstractStatement::Type::INSERT:
      return os << "INSERT";
    case AbstractStatement::Type::UPDATE:
      return os << "UPDATE";
    case AbstractStatement::Type::SELECT:
      return os << "SELECT";
    case AbstractStatement::Type::DELETE:
      return os << "DELETE";
    case AbstractStatement::Type::CREATE_VIEW:
      return os << "CREATE VIEW";
    case AbstractStatement::Type::GDPR:
      return os << "GDPR";
    default:
      LOG(FATAL) << "No string representation defined for an enum variant for "
                 << typeid(t).name();
  }
}

}  // namespace sqlast
}  // namespace pelton
