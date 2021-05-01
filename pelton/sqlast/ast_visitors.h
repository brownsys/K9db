// Helper visitors for traversing and mutating ASTs.
#ifndef PELTON_SQLAST_AST_VISITORS_H_
#define PELTON_SQLAST_AST_VISITORS_H_

#include <memory>
#include <string>
#include <utility>

#include "pelton/sqlast/ast_abstract.h"
#include "pelton/sqlast/ast_expressions.h"
#include "pelton/sqlast/ast_schema.h"
#include "pelton/sqlast/ast_statements.h"

#define PELTON_CONST_VISIT_CAST(stmt, type, visitor) \
  static_cast<const type *>(stmt)->Visit(visitor)

#define PELTON_VISIT_CAST(stmt, type, visitor) \
  static_cast<type *>(stmt)->Visit(visitor)

namespace pelton {
namespace sqlast {

// Visit without modification.
template <class T>
class AbstractVisitor {
 public:
  AbstractVisitor() = default;
  virtual T VisitCreateTable(const CreateTable &ast) = 0;
  virtual T VisitColumnDefinition(const ColumnDefinition &ast) = 0;
  virtual T VisitColumnConstraint(const ColumnConstraint &ast) = 0;
  virtual T VisitCreateIndex(const CreateIndex &ast) = 0;
  virtual T VisitInsert(const Insert &ast) = 0;
  virtual T VisitUpdate(const Update &ast) = 0;
  virtual T VisitSelect(const Select &ast) = 0;
  virtual T VisitColumnExpression(const ColumnExpression &ast) = 0;
  virtual T VisitLiteralExpression(const LiteralExpression &ast) = 0;
  virtual T VisitBinaryExpression(const BinaryExpression &ast) = 0;
  virtual T VisitDelete(const Delete &ast) = 0;

  T Visit(const AbstractStatement *stmt) {
    switch (stmt->type()) {
      case sqlast::AbstractStatement::Type::CREATE_TABLE:
        return PELTON_CONST_VISIT_CAST(stmt, CreateTable, this);
      case sqlast::AbstractStatement::Type::CREATE_INDEX:
        return PELTON_CONST_VISIT_CAST(stmt, CreateIndex, this);
      case sqlast::AbstractStatement::Type::INSERT:
        return PELTON_CONST_VISIT_CAST(stmt, Insert, this);
      case sqlast::AbstractStatement::Type::UPDATE:
        return PELTON_CONST_VISIT_CAST(stmt, Update, this);
      case sqlast::AbstractStatement::Type::DELETE:
        return PELTON_CONST_VISIT_CAST(stmt, Delete, this);
      case sqlast::AbstractStatement::Type::SELECT:
        return PELTON_CONST_VISIT_CAST(stmt, Select, this);
      case sqlast::AbstractStatement::Type::CREATE_VIEW:
      default:
        assert(false);
    }
  }
};

// Visit and modify.
template <class T>
class MutableVisitor {
 public:
  MutableVisitor() = default;
  virtual T VisitCreateTable(CreateTable *ast) = 0;
  virtual T VisitColumnDefinition(ColumnDefinition *ast) = 0;
  virtual T VisitColumnConstraint(ColumnConstraint *ast) = 0;
  virtual T VisitCreateIndex(CreateIndex *ast) = 0;
  virtual T VisitInsert(Insert *ast) = 0;
  virtual T VisitUpdate(Update *ast) = 0;
  virtual T VisitSelect(Select *ast) = 0;
  virtual T VisitColumnExpression(ColumnExpression *ast) = 0;
  virtual T VisitLiteralExpression(LiteralExpression *ast) = 0;
  virtual T VisitBinaryExpression(BinaryExpression *ast) = 0;
  virtual T VisitDelete(Delete *ast) = 0;

  T Visit(AbstractStatement *stmt) {
    switch (stmt->type()) {
      case sqlast::AbstractStatement::Type::CREATE_TABLE:
        return PELTON_VISIT_CAST(stmt, CreateTable, this);
      case sqlast::AbstractStatement::Type::CREATE_INDEX:
        return PELTON_VISIT_CAST(stmt, CreateIndex, this);
      case sqlast::AbstractStatement::Type::INSERT:
        return PELTON_VISIT_CAST(stmt, Insert, this);
      case sqlast::AbstractStatement::Type::UPDATE:
        return PELTON_VISIT_CAST(stmt, Update, this);
      case sqlast::AbstractStatement::Type::DELETE:
        return PELTON_VISIT_CAST(stmt, Delete, this);
      case sqlast::AbstractStatement::Type::SELECT:
        return PELTON_VISIT_CAST(stmt, Select, this);
      case sqlast::AbstractStatement::Type::CREATE_VIEW:
      default:
        assert(false);
    }
  }
};

// Turns ASTs to strings.
class Stringifier : public AbstractVisitor<std::string> {
 public:
  explicit Stringifier(const std::string shard_name) : shard_prefix_() {
    if (shard_name.size() > 0) {
      shard_prefix_ = shard_name + ".";
    }
  }

  std::string VisitCreateTable(const CreateTable &ast) override;
  std::string VisitColumnDefinition(const ColumnDefinition &ast) override;
  std::string VisitColumnConstraint(const ColumnConstraint &ast) override;
  std::string VisitCreateIndex(const CreateIndex &ast) override;
  std::string VisitInsert(const Insert &ast) override;
  std::string VisitUpdate(const Update &ast) override;
  std::string VisitSelect(const Select &ast) override;
  std::string VisitColumnExpression(const ColumnExpression &ast) override;
  std::string VisitLiteralExpression(const LiteralExpression &ast) override;
  std::string VisitBinaryExpression(const BinaryExpression &ast) override;
  std::string VisitDelete(const Delete &ast) override;

 private:
  std::string shard_prefix_;
};

// Finds the value assigned to a specified column anywhere in this expression.
class ValueFinder : public AbstractVisitor<std::pair<bool, std::string>> {
 public:
  explicit ValueFinder(const std::string &colname)
      : AbstractVisitor(), colname_(colname) {}

  std::pair<bool, std::string> VisitCreateTable(
      const CreateTable &ast) override;
  std::pair<bool, std::string> VisitColumnDefinition(
      const ColumnDefinition &ast) override;
  std::pair<bool, std::string> VisitColumnConstraint(
      const ColumnConstraint &ast) override;
  std::pair<bool, std::string> VisitCreateIndex(
      const CreateIndex &ast) override;
  std::pair<bool, std::string> VisitInsert(const Insert &ast) override;
  std::pair<bool, std::string> VisitUpdate(const Update &ast) override;
  std::pair<bool, std::string> VisitSelect(const Select &ast) override;
  std::pair<bool, std::string> VisitDelete(const Delete &ast) override;
  std::pair<bool, std::string> VisitColumnExpression(
      const ColumnExpression &ast) override;
  std::pair<bool, std::string> VisitLiteralExpression(
      const LiteralExpression &ast) override;
  std::pair<bool, std::string> VisitBinaryExpression(
      const BinaryExpression &ast) override;

 private:
  std::string colname_;
};

// Removes any expression/subexpression that assigns a value to the specified
// columns.
class ExpressionRemover : public MutableVisitor<std::unique_ptr<Expression>> {
 public:
  explicit ExpressionRemover(const std::string &colname)
      : MutableVisitor(), colname_(colname) {}

  std::unique_ptr<Expression> VisitCreateTable(CreateTable *ast) override;
  std::unique_ptr<Expression> VisitColumnDefinition(
      ColumnDefinition *ast) override;
  std::unique_ptr<Expression> VisitColumnConstraint(
      ColumnConstraint *ast) override;
  std::unique_ptr<Expression> VisitCreateIndex(CreateIndex *ast) override;
  std::unique_ptr<Expression> VisitInsert(Insert *ast) override;
  std::unique_ptr<Expression> VisitUpdate(Update *ast) override;
  std::unique_ptr<Expression> VisitSelect(Select *ast) override;
  std::unique_ptr<Expression> VisitDelete(Delete *ast) override;
  std::unique_ptr<Expression> VisitColumnExpression(
      ColumnExpression *ast) override;
  std::unique_ptr<Expression> VisitLiteralExpression(
      LiteralExpression *ast) override;
  std::unique_ptr<Expression> VisitBinaryExpression(
      BinaryExpression *ast) override;

 private:
  std::string colname_;
};

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_AST_VISITORS_H_
