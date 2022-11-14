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

/*
 * ColumnConstraint.
 */
std::string ColumnConstraint::TypeToString(Type type) {
  switch (type) {
    case Type::PRIMARY_KEY:
      return "PRIMARY KEY";
    case Type::UNIQUE:
      return "UNIQUE";
    case Type::FOREIGN_KEY:
      return "FOREIGN KEY";
    default:
      LOG(FATAL) << "Unsupported constraint type!";
  }
}

// Constructor is private: use these functions to create.
ColumnConstraint ColumnConstraint::Make(Type type) {
  CHECK(type != Type::FOREIGN_KEY);
  return ColumnConstraint(type);
}
ColumnConstraint ColumnConstraint::MakeForeignKey(
    const std::string &foreign_table, const std::string &foreign_column,
    FKType type) {
  ColumnConstraint constraint(Type::FOREIGN_KEY);
  constraint.data_ = ForeignKeyData(foreign_table, foreign_column, type);
  return constraint;
}

/*
 * ColumnDefinition.
 */
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
      return "TEXT";
    default:
      LOG(FATAL) << "Unsupported column type: " << type;
  }
}

void ColumnDefinition::AddConstraint(const ColumnConstraint &constraint) {
  this->constraints_.push_back(constraint);
}
const std::vector<ColumnConstraint> &ColumnDefinition::GetConstraints() const {
  return this->constraints_;
}
bool ColumnDefinition::HasConstraint(ColumnConstraint::Type type) const {
  for (const auto &constraint : this->constraints_) {
    if (constraint.type() == type) {
      return true;
    }
  }
  return false;
}
bool ColumnDefinition::HasFKType(ColumnConstraint::FKType type) const {
  for (const auto &constraint : this->constraints_) {
    if (constraint.type() == ColumnConstraint::Type::FOREIGN_KEY) {
      if (std::get<2>(constraint.ForeignKey()) == type) {
        return true;
      }
    }
  }
  return false;
}

const ColumnConstraint &ColumnDefinition::GetConstraintOfType(
    ColumnConstraint::Type type) const {
  for (const auto &constraint : this->constraints_) {
    if (type == constraint.type()) {
      return constraint;
    }
  }
  LOG(FATAL) << "No constraint of type " << type << " found for "
             << this->column_name_;
}
const ColumnConstraint &ColumnDefinition::GetForeignKeyConstraint() const {
  return this->GetConstraintOfType(ColumnConstraintTypeEnum::FOREIGN_KEY);
}

/*
 * CreateTable.
 */
void CreateTable::AddColumn(const std::string &column_name,
                            const ColumnDefinition &def) {
  this->columns_map_.insert({column_name, this->columns_.size()});
  this->columns_.push_back(def);
}
const std::vector<ColumnDefinition> &CreateTable::GetColumns() const {
  return this->columns_;
}
ColumnDefinition &CreateTable::MutableColumn(const std::string &column_name) {
  size_t column_index = this->columns_map_.at(column_name);
  return this->columns_.at(column_index);
}
bool CreateTable::HasColumn(const std::string &column_name) const {
  return this->columns_map_.count(column_name) == 1;
}

const ColumnDefinition &CreateTable::GetColumn(
    const std::string &column_name) const {
  return this->columns_.at(this->columns_map_.at(column_name));
}

void CreateTable::AddAnonymizeRule(AnonymizationRule &&anon_rule) {
  this->anon_rules_.push_back(std::move(anon_rule));
}
const std::vector<AnonymizationRule> &CreateTable::GetAnonymizationRules()
    const {
  return this->anon_rules_;
}

/*
 * Printing / logging
 */
std::ostream &operator<<(std::ostream &os, const ColumnConstraint::Type &r) {
  os << ColumnConstraint::TypeToString(r);
  return os;
}
std::ostream &operator<<(std::ostream &os, const ColumnDefinition::Type &r) {
  if (r == ColumnDefinition::Type::UINT) {
    os << "UINT";
  } else {
    os << ColumnDefinition::TypeToString(r);
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, const ColumnConstraint::FKType &t) {
  switch (t) {
    case ForeignKeyTypeEnum::OWNED_BY:
      return os << "OWNED_BY";
    case ForeignKeyTypeEnum::OWNS:
      return os << "OWNS";
    case ForeignKeyTypeEnum::ACCESSED_BY:
      return os << "ACCESSED_BY";
    case ForeignKeyTypeEnum::ACCESSES:
      return os << "ACCESSES";
    case ForeignKeyTypeEnum::AUTO:
      return os << "AUTO";
    case ForeignKeyTypeEnum::PLAIN:
      return os << "PLAIN";
  }
}

}  // namespace sqlast
}  // namespace pelton
