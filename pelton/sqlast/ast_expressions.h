// SQL expressions (e.g. inside WHERE clause).
#ifndef PELTON_SQLAST_AST_EXPRESSIONS_H_
#define PELTON_SQLAST_AST_EXPRESSIONS_H_

#include <cassert>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "pelton/sqlast/ast_abstract.h"
#include "pelton/sqlast/ast_value.h"

namespace pelton {
namespace sqlast {

// Select and delete statements.
class Expression {
 public:
  enum class Type {
    LITERAL,
    COLUMN,
    EQ,
    AND,
    GREATER_THAN,
    IN,
    LIST,
    IS,
    PLUS,
    MINUS
  };

  explicit Expression(Type type) : type_(type) {}

  virtual ~Expression() {}

  const Type &type() const { return this->type_; }

  virtual std::unique_ptr<Expression> Clone() const = 0;

  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const;

  template <class T>
  T Visit(MutableVisitor<T> *visitor);

 private:
  Type type_;
};

class ColumnExpression : public Expression {
 public:
  explicit ColumnExpression(const std::string &column)
      : Expression(Expression::Type::COLUMN), column_(column) {}

  std::unique_ptr<Expression> Clone() const override {
    return std::make_unique<ColumnExpression>(*this);
  }

  const std::string &column() const;

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
  explicit LiteralExpression(const Value &value)
      : Expression(Expression::Type::LITERAL), value_(value) {}

  const Value &value() const;

  std::unique_ptr<Expression> Clone() const override {
    return std::make_unique<LiteralExpression>(*this);
  }

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
  Value value_;
};

class LiteralListExpression : public Expression {
 public:
  explicit LiteralListExpression(const std::vector<Value> &values)
      : Expression(Expression::Type::LIST), values_(values) {}

  explicit LiteralListExpression(std::vector<Value> &&values)
      : Expression(Expression::Type::LIST), values_(std::move(values)) {}

  const std::vector<Value> &values() const;

  std::unique_ptr<Expression> Clone() const override {
    return std::make_unique<LiteralListExpression>(*this);
  }

  template <class T>
  T Visit(AbstractVisitor<T> *visitor) const {
    return visitor->VisitLiteralListExpression(*this);
  }
  template <class T>
  T Visit(MutableVisitor<T> *visitor) {
    return visitor->VisitLiteralListExpression(this);
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
  std::vector<Value> values_;
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
  const Expression *const GetRight() const;

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
    result.push_back(this->left_->Visit(visitor));
    result.push_back(this->right_->Visit(visitor));
    return result;
  }

  template <class T>
  std::vector<T> VisitChildren(MutableVisitor<T> *visitor) {
    std::vector<T> result;
    result.push_back(this->left_->Visit(visitor));
    result.push_back(this->right_->Visit(visitor));
    return result;
  }

 private:
  std::unique_ptr<Expression> left_;
  std::unique_ptr<Expression> right_;
};

// Implemented here so that expression classes are defined.
template <class T>
T Expression::Visit(AbstractVisitor<T> *visitor) const {
  switch (this->type_) {
    case Expression::Type::COLUMN:
      return static_cast<const ColumnExpression *>(this)->Visit(visitor);
    case Expression::Type::LITERAL:
      return static_cast<const LiteralExpression *>(this)->Visit(visitor);
    case Expression::Type::LIST:
      return static_cast<const LiteralListExpression *>(this)->Visit(visitor);
    case Expression::Type::EQ:
    case Expression::Type::AND:
    case Expression::Type::GREATER_THAN:
    case Expression::Type::IN:
    case Expression::Type::IS:
    case Expression::Type::PLUS:
      return static_cast<const BinaryExpression *>(this)->Visit(visitor);
    default:
      assert(false);
  }
}

template <class T>
T Expression::Visit(MutableVisitor<T> *visitor) {
  switch (this->type_) {
    case Expression::Type::COLUMN:
      return static_cast<ColumnExpression *>(this)->Visit(visitor);
    case Expression::Type::LITERAL:
      return static_cast<LiteralExpression *>(this)->Visit(visitor);
    case Expression::Type::LIST:
      return static_cast<LiteralListExpression *>(this)->Visit(visitor);
    case Expression::Type::EQ:
    case Expression::Type::AND:
    case Expression::Type::GREATER_THAN:
    case Expression::Type::IN:
    case Expression::Type::IS:
    case Expression::Type::PLUS:
      return static_cast<BinaryExpression *>(this)->Visit(visitor);
    default:
      assert(false);
  }
}

std::ostream &operator<<(std::ostream &os, const Expression &e);

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_AST_EXPRESSIONS_H_
