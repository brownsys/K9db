// SQL expressions (e.g. inside WHERE clause).
#ifndef PELTON_SQLAST_AST_EXPRESSIONS_H_
#define PELTON_SQLAST_AST_EXPRESSIONS_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "pelton/sqlast/ast_abstract.h"

namespace pelton {
namespace sqlast {

// Select and delete statements.
class Expression {
 public:
  enum class Type { LITERAL, COLUMN, EQ, AND };

  explicit Expression(Type type) : type_(type) {}

  virtual ~Expression() {}

  const Type &type() const { return this->type_; }

  virtual std::unique_ptr<Expression> Clone() const = 0;

 private:
  Type type_;
};

class ColumnExpression : public Expression {
 public:
  explicit ColumnExpression(const std::string &column)
      : Expression(Expression::Type::COLUMN), column_(column) {}

  ColumnExpression(const ColumnExpression &expr)
      : Expression(Expression::Type::COLUMN) {
    this->column_ = expr.column_;
  }

  std::unique_ptr<Expression> Clone() const override {
    return std::make_unique<ColumnExpression>(*this);
  }

  const std::string &column() const;
  std::string &column();

  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitColumnExpression(*this);
  }
  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitColumnExpression(this);
  }
  template <class T>
  std::vector<T> VisitChildren(AbstractVisitor<T> *visitor) const {
    return {};
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    return {};
  }

 private:
  std::string column_;
};

class LiteralExpression : public Expression {
 public:
  explicit LiteralExpression(const std::string &value)
      : Expression(Expression::Type::LITERAL), value_(value) {}

  LiteralExpression(const LiteralExpression &expr)
      : Expression(Expression::Type::LITERAL) {
    this->value_ = expr.value_;
  }

  std::unique_ptr<Expression> Clone() const override {
    return std::make_unique<LiteralExpression>(*this);
  }

  const std::string &value() const;
  std::string &value();

  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitLiteralExpression(*this);
  }
  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitLiteralExpression(this);
  }
  template <class T>
  std::vector<T> VisitChildren(AbstractVisitor<T> *visitor) const {
    return {};
  }
  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    return {};
  }

 private:
  std::string value_;
};

class BinaryExpression : public Expression {
 public:
  explicit BinaryExpression(Expression::Type type) : Expression(type) {}

  BinaryExpression(const BinaryExpression &expr) : Expression(expr.type()) {
    this->left_ = expr.GetLeft()->Clone();
    this->right_ = expr.GetRight()->Clone();
  }

  std::unique_ptr<Expression> Clone() const override {
    return std::make_unique<BinaryExpression>(*this);
  }

  const Expression *const GetLeft() const;
  Expression *const GetLeft();
  const Expression *const GetRight() const;
  Expression *const GetRight();

  void SetLeft(std::unique_ptr<Expression> &&left);
  void SetRight(std::unique_ptr<Expression> &&right);

  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitBinaryExpression(*this);
  }

  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitBinaryExpression(this);
  }

  template <class T>
  std::vector<T> VisitChildren(AbstractVisitor<T> *visitor) const {
    std::vector<T> result;
    switch (this->left_->type()) {
      case Expression::Type::COLUMN:
        result.push_back(
            std::move(static_cast<ColumnExpression *>(this->left_.get())
                          ->Visit(visitor)));
        break;
      case Expression::Type::LITERAL:
        result.push_back(
            std::move(static_cast<LiteralExpression *>(this->left_.get())
                          ->Visit(visitor)));
        break;
      default:
        result.push_back(
            std::move(static_cast<BinaryExpression *>(this->left_.get())
                          ->Visit(visitor)));
    }
    switch (this->right_->type()) {
      case Expression::Type::COLUMN:
        result.push_back(
            std::move(static_cast<ColumnExpression *>(this->right_.get())
                          ->Visit(visitor)));
        break;
      case Expression::Type::LITERAL:
        result.push_back(
            std::move(static_cast<LiteralExpression *>(this->right_.get())
                          ->Visit(visitor)));
        break;
      default:
        result.push_back(
            std::move(static_cast<BinaryExpression *>(this->right_.get())
                          ->Visit(visitor)));
    }
    return result;
  }

  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    std::vector<T> result;
    switch (this->left_->type()) {
      case Expression::Type::COLUMN:
        result.push_back(
            std::move(static_cast<ColumnExpression *>(this->left_.get())
                          ->Visit(visitor)));
        break;
      case Expression::Type::LITERAL:
        result.push_back(
            std::move(static_cast<LiteralExpression *>(this->left_.get())
                          ->Visit(visitor)));
        break;
      default:
        result.push_back(
            std::move(static_cast<BinaryExpression *>(this->left_.get())
                          ->Visit(visitor)));
    }
    switch (this->right_->type()) {
      case Expression::Type::COLUMN:
        result.push_back(
            std::move(static_cast<ColumnExpression *>(this->right_.get())
                          ->Visit(visitor)));
        break;
      case Expression::Type::LITERAL:
        result.push_back(
            std::move(static_cast<LiteralExpression *>(this->right_.get())
                          ->Visit(visitor)));
        break;
      default:
        result.push_back(
            std::move(static_cast<BinaryExpression *>(this->right_.get())
                          ->Visit(visitor)));
    }
    return result;
  }

 private:
  std::unique_ptr<Expression> left_;
  std::unique_ptr<Expression> right_;
};

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_AST_EXPRESSIONS_H_
