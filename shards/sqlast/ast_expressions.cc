// SQL expressions (e.g. inside WHERE clause).
#include "shards/sqlast/ast_expressions.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace shards {
namespace sqlast {

// ColumnExpression.
const std::string &ColumnExpression::column() const { return this->column_; }
std::string &ColumnExpression::column() { return this->column_; }

// LiteralExpression.
const std::string &LiteralExpression::value() const { return this->value_; }
std::string &LiteralExpression::value() { return this->value_; }

// BinaryExpression.
const Expression *const BinaryExpression::GetLeft() const {
  return this->left_.get();
}
Expression *const BinaryExpression::GetLeft() { return this->left_.get(); }
const Expression *const BinaryExpression::GetRight() const {
  return this->right_.get();
}
Expression *const BinaryExpression::GetRight() { return this->right_.get(); }

void BinaryExpression::SetLeft(std::unique_ptr<Expression> &&left) {
  this->left_ = std::move(left);
}
void BinaryExpression::SetRight(std::unique_ptr<Expression> &&right) {
  this->right_ = std::move(right);
}

}  // namespace sqlast
}  // namespace shards
