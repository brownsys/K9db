// Insert, delete and select statements.
#include "shards/sqlast/ast_statements.h"

#include <algorithm>
#include <iterator>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace shards {
namespace sqlast {

// Insert.
const std::string &Insert::table_name() const { return this->table_name_; }
std::string &Insert::table_name() { return this->table_name_; }

bool Insert::HasColumns() { return this->columns_.size() > 0; }

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

std::string Insert::RemoveValue(const std::string &colname) {
  if (this->columns_.size() > 0) {
    auto it = std::find(this->columns_.begin(), this->columns_.end(), colname);
    if (it == this->columns_.end()) {
      throw "Insert statement does not contain column \"" + colname + "\"";
    }
    this->columns_.erase(it);
    size_t index = std::distance(this->columns_.begin(), it);
    std::string result = this->values_.at(index);
    this->values_.erase(this->values_.begin() + index);
    return result;
  }
  throw "Insert statement does not have column names set explicitly";
}
std::string Insert::RemoveValue(size_t index) {
  if (this->columns_.size() > 0) {
    this->columns_.erase(this->columns_.begin() + index);
  }
  std::string result = this->values_.at(index);
  this->values_.erase(this->values_.begin() + index);
  return result;
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
  return this->where_clause_->get();
}
BinaryExpression *const Select::GetWhereClause() {
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
  return this->where_clause_->get();
}
BinaryExpression *const Delete::GetWhereClause() {
  return this->where_clause_->get();
}
void Delete::SetWhereClause(std::unique_ptr<BinaryExpression> &&where) {
  this->where_clause_ = std::optional(std::move(where));
}
void Delete::RemoveWhereClause() {
  this->where_clause_ = std::optional<std::unique_ptr<BinaryExpression>>();
}

}  // namespace sqlast
}  // namespace shards
