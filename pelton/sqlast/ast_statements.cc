// Insert, delete and select statements.
#include "pelton/sqlast/ast_statements.h"

#include <algorithm>
#include <iterator>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace pelton {
namespace sqlast {

// Insert.
const std::string &Insert::table_name() const { return this->table_name_; }
std::string &Insert::table_name() { return this->table_name_; }

bool Insert::HasColumns() const { return this->columns_.size() > 0; }

void Insert::AddColumn(const std::string &colname) {
  this->columns_.push_back(colname);
}
const std::vector<std::string> &Insert::GetColumns() const {
  return this->columns_;
}

void Insert::SetValues(std::vector<std::string> &&values) {
  this->values_ = values;
}
const std::vector<std::string> &Insert::GetValues() const {
  return this->values_;
}

void Insert::AddValue(const std::string &val) { this->values_.push_back(val); }
void Insert::AddValue(std::string &&val) {
  this->values_.push_back(std::move(val));
}

absl::StatusOr<std::string> Insert::RemoveValue(const std::string &colname) {
  if (this->columns_.size() > 0) {
    auto it = std::find(this->columns_.begin(), this->columns_.end(), colname);
    if (it == this->columns_.end()) {
      return absl::InvalidArgumentError(
          "Insert statement does not contain column \"" + colname + "\"");
    }
    this->columns_.erase(it);
    size_t index = std::distance(this->columns_.begin(), it);
    std::string result = this->values_.at(index);
    this->values_.erase(this->values_.begin() + index);
    return result;
  }
  return absl::InvalidArgumentError(
      "Insert statement does not have column names set explicitly");
}
std::string Insert::RemoveValue(size_t index) {
  if (this->columns_.size() > 0) {
    this->columns_.erase(this->columns_.begin() + index);
  }
  std::string result = this->values_.at(index);
  this->values_.erase(this->values_.begin() + index);
  return result;
}

absl::StatusOr<std::string> Insert::GetValue(const std::string &colname) {
  if (this->columns_.size() > 0) {
    auto it = std::find(this->columns_.begin(), this->columns_.end(), colname);
    if (it == this->columns_.end()) {
      return absl::InvalidArgumentError(
          "Insert statement does not contain column \"" + colname + "\"");
    }
    size_t index = std::distance(this->columns_.begin(), it);
    return this->values_.at(index);
  }
  return absl::InvalidArgumentError(
      "Insert statement does not have column names set explicitly");
}
const std::string &Insert::GetValue(size_t index) {
  return this->values_.at(index);
}

// Update.
const std::string &Update::table_name() const { return this->table_name_; }
std::string &Update::table_name() { return this->table_name_; }

void Update::AddColumnValue(const std::string &column,
                            const std::string &value) {
  this->columns_.push_back(column);
  this->values_.push_back(value);
}
absl::StatusOr<std::string> Update::RemoveColumnValue(
    const std::string &column) {
  auto it = std::find(this->columns_.begin(), this->columns_.end(), column);
  if (it == this->columns_.end()) {
    return absl::InvalidArgumentError(
        "Update statement does not contain column \"" + column + "\"");
  }
  this->columns_.erase(it);
  size_t index = std::distance(this->columns_.begin(), it);
  std::string result = this->values_.at(index);
  this->values_.erase(this->values_.begin() + index);
  return result;
}
bool Update::AssignsTo(const std::string &column) const {
  return std::find(this->columns_.begin(), this->columns_.end(), column) !=
         this->columns_.end();
}

const std::vector<std::string> &Update::GetColumns() const {
  return this->columns_;
}
const std::vector<std::string> &Update::GetValues() const {
  return this->values_;
}
std::vector<std::string> &Update::GetColumns() { return this->columns_; }
std::vector<std::string> &Update::GetValues() { return this->values_; }

bool Update::HasWhereClause() const { return this->where_clause_.has_value(); }
const BinaryExpression *const Update::GetWhereClause() const {
  if (!this->where_clause_.has_value()) {
    return nullptr;
  }
  return this->where_clause_->get();
}
BinaryExpression *const Update::GetWhereClause() {
  if (!this->where_clause_.has_value()) {
    return nullptr;
  }
  return this->where_clause_->get();
}
void Update::SetWhereClause(std::unique_ptr<BinaryExpression> &&where) {
  this->where_clause_ = std::optional(std::move(where));
}
void Update::RemoveWhereClause() {
  this->where_clause_ = std::optional<std::unique_ptr<BinaryExpression>>();
}

Select Update::SelectDomain() const {
  Select select{this->table_name()};
  select.AddColumn("*");
  if (this->HasWhereClause()) {
    select.SetWhereClause(
        std::make_unique<BinaryExpression>(*this->where_clause_->get()));
  }
  return select;
}

Delete Update::DeleteDomain() const {
  Delete del{this->table_name()};
  if (this->HasWhereClause()) {
    del.SetWhereClause(
        std::make_unique<BinaryExpression>(*this->where_clause_->get()));
  }
  return del;
}

// Select.
const std::string &Select::table_name() const { return this->table_name_; }
std::string &Select::table_name() { return this->table_name_; }

void Select::AddColumn(const std::string &column) {
  this->columns_.push_back(column);
}
const std::vector<std::string> &Select::GetColumns() const {
  return this->columns_;
}
std::vector<std::string> &Select::GetColumns() { return this->columns_; }
void Select::RemoveColumn(const std::string &column) {
  this->columns_.erase(
      std::find(this->columns_.begin(), this->columns_.end(), column));
}

bool Select::HasWhereClause() const { return this->where_clause_.has_value(); }
const BinaryExpression *const Select::GetWhereClause() const {
  if (!this->where_clause_.has_value()) {
    return nullptr;
  }
  return this->where_clause_->get();
}
BinaryExpression *const Select::GetWhereClause() {
  if (!this->where_clause_.has_value()) {
    return nullptr;
  }
  return this->where_clause_->get();
}
void Select::SetWhereClause(std::unique_ptr<BinaryExpression> &&where) {
  this->where_clause_ = std::optional(std::move(where));
}
void Select::RemoveWhereClause() {
  this->where_clause_ = std::optional<std::unique_ptr<BinaryExpression>>();
}

// Delete.
const std::string &Delete::table_name() const { return this->table_name_; }
std::string &Delete::table_name() { return this->table_name_; }

bool Delete::HasWhereClause() const { return this->where_clause_.has_value(); }
const BinaryExpression *const Delete::GetWhereClause() const {
  if (!this->where_clause_.has_value()) {
    return nullptr;
  }
  return this->where_clause_->get();
}
BinaryExpression *const Delete::GetWhereClause() {
  if (!this->where_clause_.has_value()) {
    return nullptr;
  }
  return this->where_clause_->get();
}
void Delete::SetWhereClause(std::unique_ptr<BinaryExpression> &&where) {
  this->where_clause_ = std::optional(std::move(where));
}
void Delete::RemoveWhereClause() {
  this->where_clause_ = std::optional<std::unique_ptr<BinaryExpression>>();
}

Select Delete::SelectDomain() const {
  Select select{this->table_name()};
  select.AddColumn("*");
  if (this->HasWhereClause()) {
    select.SetWhereClause(
        std::make_unique<BinaryExpression>(*this->where_clause_->get()));
  }
  return select;
}

}  // namespace sqlast
}  // namespace pelton
