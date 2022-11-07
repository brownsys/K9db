#include "pelton/sqlast/value_mapper.h"

#include "glog/logging.h"

namespace pelton {
namespace sqlast {

#define IS_COLUMN(expr) expr->type() == Expression::Type::COLUMN
#define IS_LITERAL(expr) expr->type() == Expression::Type::LITERAL
#define IS_LIST(expr) expr->type() == Expression::Type::LIST
#define TO_COLUMN(expr) reinterpret_cast<const ColumnExpression *>(expr)
#define TO_LITERAL(expr) reinterpret_cast<const LiteralExpression *>(expr)
#define TO_LIST(expr) reinterpret_cast<const LiteralListExpression *>(expr)
#define TO_BINARY(expr) reinterpret_cast<const BinaryExpression *>(expr)

// These are unsupported.
void ValueMapper::VisitCreateTable(const CreateTable &ast) {
  LOG(FATAL) << "UNSUPPORTED";
}
void ValueMapper::VisitColumnDefinition(const ColumnDefinition &ast) {
  LOG(FATAL) << "UNSUPPORTED";
}
void ValueMapper::VisitColumnConstraint(const ColumnConstraint &ast) {
  LOG(FATAL) << "UNSUPPORTED";
}
void ValueMapper::VisitCreateIndex(const CreateIndex &ast) {
  LOG(FATAL) << "UNSUPPORTED";
}
void ValueMapper::VisitCreateView(const CreateView &ast) {
  LOG(FATAL) << "UNSUPPORTED";
}
void ValueMapper::VisitInsert(const Insert &ast) {
  LOG(FATAL) << "UNSUPPORTED";
}
void ValueMapper::VisitReplace(const Replace &ast) {
  LOG(FATAL) << "UNSUPPORTED";
}
void ValueMapper::VisitUpdate(const Update &ast) {
  LOG(FATAL) << "UNSUPPORTED";
}
void ValueMapper::VisitGDPRStatement(const GDPRStatement &ast) {
  LOG(FATAL) << "UNSUPPORTED";
}

// These are important.
void ValueMapper::VisitSelect(const Select &ast) {
  if (ast.HasWhereClause()) {
    ast.GetWhereClause()->Visit(this);
  }
}
void ValueMapper::VisitDelete(const Delete &ast) {
  if (ast.HasWhereClause()) {
    ast.GetWhereClause()->Visit(this);
  }
}

// Binary expression.
void ValueMapper::VisitBinaryExpression(const BinaryExpression &ast) {
  switch (ast.type()) {
    case Expression::Type::EQ: {
      CHECK(IS_COLUMN(ast.GetLeft()) || IS_COLUMN(ast.GetRight())) << "EQ";
      CHECK(IS_LITERAL(ast.GetLeft()) || IS_LITERAL(ast.GetRight())) << "EQ";

      const ColumnExpression *col;
      const LiteralExpression *val;
      if (IS_COLUMN(ast.GetLeft())) {
        col = TO_COLUMN(ast.GetLeft());
        val = TO_LITERAL(ast.GetRight());
      } else {
        col = TO_COLUMN(ast.GetRight());
        val = TO_LITERAL(ast.GetLeft());
      }

      size_t col_idx = this->schema_.IndexOf(col->column());
      std::vector<Value> &vec = this->values_[col_idx];
      CHECK_EQ(vec.size(), 0u) << "Too many constraints on " << col->column();
      vec.push_back(val->value());
      break;
    }
    case Expression::Type::IS: {
      CHECK(IS_COLUMN(ast.GetLeft())) << "IS";
      CHECK(IS_LITERAL(ast.GetRight())) << "IS";

      const ColumnExpression *col = TO_COLUMN(ast.GetLeft());
      const LiteralExpression *val = TO_LITERAL(ast.GetRight());
      size_t col_idx = this->schema_.IndexOf(col->column());
      std::vector<Value> &vec = this->values_[col_idx];
      CHECK_EQ(vec.size(), 0u) << "Too many constraints on " << col->column();
      vec.push_back(val->value());
      break;
    }
    case Expression::Type::IN: {
      CHECK(IS_COLUMN(ast.GetLeft())) << "IN";
      CHECK(IS_LIST(ast.GetRight())) << "IN";

      const ColumnExpression *col = TO_COLUMN(ast.GetLeft());
      const LiteralListExpression *list = TO_LIST(ast.GetRight());
      size_t col_idx = this->schema_.IndexOf(col->column());
      CHECK_EQ(this->values_.count(col_idx), 0u)
          << "Too many constraints on " << col->column();
      this->values_.emplace(col_idx, list->values());
      break;
    }
    case Expression::Type::AND: {
      this->VisitBinaryExpression(*TO_BINARY(ast.GetLeft()));
      this->VisitBinaryExpression(*TO_BINARY(ast.GetRight()));
      break;
    }
    default:  // >, LITERAL, CLUMN, LIST.
      LOG(FATAL) << "Bad expression";
  }
}

// These will never be invoked.
void ValueMapper::VisitColumnExpression(const ColumnExpression &ast) {
  LOG(FATAL) << "UNREACHABLE";
}
void ValueMapper::VisitLiteralExpression(const LiteralExpression &ast) {
  LOG(FATAL) << "UNREACHABLE";
}
void ValueMapper::VisitLiteralListExpression(const LiteralListExpression &ast) {
  LOG(FATAL) << "UNREACHABLE";
}

}  // namespace sqlast
}  // namespace pelton
