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
  virtual T VisitAnonymizationRule(const AnonymizationRule &ast) = 0;
  virtual T VisitCreateIndex(const CreateIndex &ast) = 0;
  virtual T VisitCreateView(const CreateView &ast) = 0;
  virtual T VisitInsert(const Insert &ast) = 0;
  virtual T VisitReplace(const Replace &ast) = 0;
  virtual T VisitUpdate(const Update &ast) = 0;
  virtual T VisitSelect(const Select &ast) = 0;
  virtual T VisitDelete(const Delete &ast) = 0;
  virtual T VisitGDPRStatement(const GDPRStatement &ast) = 0;
  virtual T VisitExplainQuery(const ExplainQuery &ast) = 0;
  virtual T VisitColumnExpression(const ColumnExpression &ast) = 0;
  virtual T VisitLiteralExpression(const LiteralExpression &ast) = 0;
  virtual T VisitLiteralListExpression(const LiteralListExpression &ast) = 0;
  virtual T VisitBinaryExpression(const BinaryExpression &ast) = 0;
};

// Visit and modify.
template <class T>
class MutableVisitor {
 public:
  MutableVisitor() = default;
  virtual T VisitCreateTable(CreateTable *ast) = 0;
  virtual T VisitColumnDefinition(ColumnDefinition *ast) = 0;
  virtual T VisitColumnConstraint(ColumnConstraint *ast) = 0;
  virtual T VisitAnonymizationRule(AnonymizationRule *ast) = 0;
  virtual T VisitCreateIndex(CreateIndex *ast) = 0;
  virtual T VisitCreateView(CreateView *ast) = 0;
  virtual T VisitInsert(Insert *ast) = 0;
  virtual T VisitReplace(Replace *ast) = 0;
  virtual T VisitUpdate(Update *ast) = 0;
  virtual T VisitSelect(Select *ast) = 0;
  virtual T VisitDelete(Delete *ast) = 0;
  virtual T VisitGDPRStatement(GDPRStatement *ast) = 0;
  virtual T VisitExplainQuery(ExplainQuery *ast) = 0;
  virtual T VisitColumnExpression(ColumnExpression *ast) = 0;
  virtual T VisitLiteralExpression(LiteralExpression *ast) = 0;
  virtual T VisitLiteralListExpression(LiteralListExpression *ast) = 0;
  virtual T VisitBinaryExpression(BinaryExpression *ast) = 0;
};

// Allow us to visit a statement without knowing its exact type.
#define PELTON_VISIT_CAST(type) static_cast<type *>(this)->Visit(visitor)

template <class T>
T AbstractStatement::Visit(AbstractVisitor<T> *visitor) const {
  switch (this->type()) {
    case sqlast::AbstractStatement::Type::CREATE_TABLE:
      return PELTON_VISIT_CAST(const CreateTable);
    case sqlast::AbstractStatement::Type::CREATE_INDEX:
      return PELTON_VISIT_CAST(const CreateIndex);
    case sqlast::AbstractStatement::Type::CREATE_VIEW:
      return PELTON_VISIT_CAST(const CreateView);
    case sqlast::AbstractStatement::Type::INSERT:
      return PELTON_VISIT_CAST(const Insert);
    case sqlast::AbstractStatement::Type::REPLACE:
      return PELTON_VISIT_CAST(const Replace);
    case sqlast::AbstractStatement::Type::UPDATE:
      return PELTON_VISIT_CAST(const Update);
    case sqlast::AbstractStatement::Type::DELETE:
      return PELTON_VISIT_CAST(const Delete);
    case sqlast::AbstractStatement::Type::SELECT:
      return PELTON_VISIT_CAST(const Select);
    case sqlast::AbstractStatement::Type::GDPR:
      return PELTON_VISIT_CAST(const GDPRStatement);
    case sqlast::AbstractStatement::Type::EXPLAIN_QUERY:
      return PELTON_VISIT_CAST(const ExplainQuery);
    default:
      assert(false);
  }
}

template <class T>
T AbstractStatement::Visit(MutableVisitor<T> *visitor) {
  switch (this->type()) {
    case sqlast::AbstractStatement::Type::CREATE_TABLE:
      return PELTON_VISIT_CAST(CreateTable);
    case sqlast::AbstractStatement::Type::CREATE_INDEX:
      return PELTON_VISIT_CAST(CreateIndex);
    case sqlast::AbstractStatement::Type::CREATE_VIEW:
      return PELTON_VISIT_CAST(CreateView);
    case sqlast::AbstractStatement::Type::INSERT:
      return PELTON_VISIT_CAST(Insert);
    case sqlast::AbstractStatement::Type::REPLACE:
      return PELTON_VISIT_CAST(Replace);
    case sqlast::AbstractStatement::Type::UPDATE:
      return PELTON_VISIT_CAST(Update);
    case sqlast::AbstractStatement::Type::DELETE:
      return PELTON_VISIT_CAST(Delete);
    case sqlast::AbstractStatement::Type::SELECT:
      return PELTON_VISIT_CAST(Select);
    case sqlast::AbstractStatement::Type::GDPR:
      return PELTON_VISIT_CAST(GDPRStatement);
    case sqlast::AbstractStatement::Type::EXPLAIN_QUERY:
      return PELTON_VISIT_CAST(ExplainQuery);
    default:
      assert(false);
  }
}

// Turns ASTs to strings.
class Stringifier : public AbstractVisitor<std::string> {
 public:
  Stringifier() = default;

  std::string VisitCreateTable(const CreateTable &ast) override;
  std::string VisitColumnDefinition(const ColumnDefinition &ast) override;
  std::string VisitColumnConstraint(const ColumnConstraint &ast) override;
  std::string VisitAnonymizationRule(const AnonymizationRule &ast) override;
  std::string VisitCreateIndex(const CreateIndex &ast) override;
  std::string VisitCreateView(const CreateView &ast) override;
  std::string VisitInsert(const Insert &ast) override;
  std::string VisitReplace(const Replace &ast) override;
  std::string VisitUpdate(const Update &ast) override;
  std::string VisitSelect(const Select &ast) override;
  std::string VisitDelete(const Delete &ast) override;
  std::string VisitGDPRStatement(const GDPRStatement &ast) override;
  std::string VisitExplainQuery(const ExplainQuery &ast) override;
  std::string VisitColumnExpression(const ColumnExpression &ast) override;
  std::string VisitLiteralExpression(const LiteralExpression &ast) override;
  std::string VisitLiteralListExpression(
      const LiteralListExpression &ast) override;
  std::string VisitBinaryExpression(const BinaryExpression &ast) override;
};

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_AST_VISITORS_H_
