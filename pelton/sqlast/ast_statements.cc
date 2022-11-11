// Insert, delete and select statements.
#include "pelton/sqlast/ast_statements.h"

#include <algorithm>
#include <iterator>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"

namespace pelton {
namespace sqlast {

/*
 * Insert.
 */
bool Insert::HasColumns() const { return this->columns_.size() > 0; }
void Insert::SetColumns(std::vector<std::string> &&colname) {
  this->columns_ = std::move(colname);
}
void Insert::SetValues(std::vector<Value> &&values) {
  this->values_ = std::move(values);
}
const Value &Insert::GetValue(const std::string &colname, size_t index) const {
  if (this->HasColumns()) {
    for (size_t i = 0; i < this->columns_.size(); i++) {
      if (this->columns_.at(i) == colname) {
        return this->values_.at(i);
      }
    }
    LOG(FATAL) << "Insert statement has no value for column " << colname;
  } else {
    if (index >= this->values_.size()) {
      LOG(FATAL) << "Insert statement has no value at index " << index;
    }
    return this->values_.at(index);
  }
}

// Update.
void Update::AddColumnValue(const std::string &column,
                            std::unique_ptr<Expression> &&value) {
  this->columns_.push_back(column);
  this->values_.push_back(std::move(value));
}
void Update::SetWhereClause(std::unique_ptr<BinaryExpression> &&where) {
  this->where_clause_ = std::optional(std::move(where));
}
const BinaryExpression *Update::GetWhereClause() const {
  if (this->where_clause_.has_value()) {
    return this->where_clause_->get();
  }
  return nullptr;
}
Delete Update::DeleteDomain() const {
  Delete del{this->table_name()};
  if (this->where_clause_.has_value()) {
    del.SetWhereClause(
        std::make_unique<BinaryExpression>(*this->where_clause_->get()));
  }
  return del;
}

// Select.
void Select::AddColumn(const ResultColumn &column) {
  this->columns_.push_back(column);
}
void Select::SetWhereClause(std::unique_ptr<BinaryExpression> &&where) {
  this->where_clause_ = std::optional(std::move(where));
}
const BinaryExpression *Select::GetWhereClause() const {
  if (this->where_clause_.has_value()) {
    return this->where_clause_->get();
  }
  return nullptr;
}

// Delete.
void Delete::SetWhereClause(std::unique_ptr<BinaryExpression> &&where) {
  this->where_clause_ = std::optional(std::move(where));
}
const BinaryExpression *Delete::GetWhereClause() const {
  if (this->where_clause_.has_value()) {
    return this->where_clause_->get();
  }
  return nullptr;
}

}  // namespace sqlast
}  // namespace pelton
