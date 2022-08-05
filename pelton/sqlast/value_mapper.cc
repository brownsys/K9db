#include "pelton/sqlast/value_mapper.h"

#include "glog/logging.h"

namespace pelton {
namespace sqlast {

#define IS_COLUMN(expr) expr->type() == sqlast::Expression::Type::COLUMN
#define IS_LITERAL(expr) expr->type() == sqlast::Expression::Type::LITERAL
#define IS_LIST(expr) expr->type() == sqlast::Expression::Type::LIST
#define TO_COLUMN(expr) reinterpret_cast<const sqlast::ColumnExpression *>(expr)
#define TO_LITERAL(expr) \
  reinterpret_cast<const sqlast::LiteralExpression *>(expr)
#define TO_LIST(expr) \
  reinterpret_cast<const sqlast::LiteralListExpression *>(expr)
#define TO_BINARY(expr) reinterpret_cast<const sqlast::BinaryExpression *>(expr)

// These are unsupported.
void ValueMapper::VisitCreateTable(const sqlast::CreateTable &ast) {
  LOG(FATAL) << "UNSUPPORTED";
}
void ValueMapper::VisitColumnDefinition(const sqlast::ColumnDefinition &ast) {
  LOG(FATAL) << "UNSUPPORTED";
}
void ValueMapper::VisitColumnConstraint(const sqlast::ColumnConstraint &ast) {
  LOG(FATAL) << "UNSUPPORTED";
}
void ValueMapper::VisitCreateIndex(const sqlast::CreateIndex &ast) {
  LOG(FATAL) << "UNSUPPORTED";
}

// These are important.
void ValueMapper::VisitInsert(const sqlast::Insert &ast) {
  CHECK_EQ(this->schema_.keys().size(), 1u)
      << "Schema has illegal PK " << this->schema_;
  size_t pk_idx = this->schema_.keys().at(0);

  // What gets inserted.
  if (ast.HasColumns()) {
    const std::vector<std::string> &cols = ast.GetColumns();
    for (size_t i = 0; i < cols.size(); i++) {
      const std::string &name = cols.at(i);
      this->after_.emplace(this->schema_.IndexOf(name), ast.GetValue(i));
      if (ast.replace() && name == this->schema_.NameOf(pk_idx)) {
        this->before_[pk_idx].push_back(ast.GetValue(i));
      }
    }
    if (ast.replace() && this->before_.count(pk_idx) == 0) {
      LOG(FATAL) << "Replace has no PK";
    }
  } else {
    for (size_t i = 0; i < this->schema_.size(); i++) {
      this->after_.emplace(i, ast.GetValue(i));
    }
    if (ast.replace()) {
      this->before_[pk_idx].push_back(ast.GetValue(pk_idx));
    }
  }
}
void ValueMapper::VisitUpdate(const sqlast::Update &ast) {
  // Update specify after values.
  const std::vector<std::string> &cols = ast.GetColumns();
  const std::vector<std::string> &values = ast.GetValues();
  for (size_t i = 0; i < cols.size(); i++) {
    this->after_.emplace(this->schema_.IndexOf(cols.at(i)), values.at(i));
  }

  // Where condition specifies before value.
  if (ast.HasWhereClause()) {
    this->VisitBinaryExpression(*ast.GetWhereClause());
  }
}
void ValueMapper::VisitSelect(const sqlast::Select &ast) {
  if (ast.HasWhereClause()) {
    this->VisitBinaryExpression(*ast.GetWhereClause());
  }
}
void ValueMapper::VisitDelete(const sqlast::Delete &ast) {
  if (ast.HasWhereClause()) {
    this->VisitBinaryExpression(*ast.GetWhereClause());
  }
}

// Binary expression.
void ValueMapper::VisitBinaryExpression(const sqlast::BinaryExpression &ast) {
  switch (ast.type()) {
    case sqlast::Expression::Type::EQ: {
      CHECK(IS_COLUMN(ast.GetLeft()) || IS_COLUMN(ast.GetRight())) << "EQ";
      CHECK(IS_LITERAL(ast.GetLeft()) || IS_LITERAL(ast.GetRight())) << "EQ";

      const sqlast::ColumnExpression *col;
      const sqlast::LiteralExpression *val;
      if (IS_COLUMN(ast.GetLeft())) {
        col = TO_COLUMN(ast.GetLeft());
        val = TO_LITERAL(ast.GetRight());
      } else {
        col = TO_COLUMN(ast.GetRight());
        val = TO_LITERAL(ast.GetLeft());
      }

      size_t col_idx = this->schema_.IndexOf(col->column());
      std::vector<std::string> &vec = this->before_[col_idx];
      CHECK_EQ(vec.size(), 0u) << "Too many constraints on " << col->column();
      vec.push_back(val->value());
      break;
    }
    case sqlast::Expression::Type::IS: {
      CHECK(IS_COLUMN(ast.GetLeft())) << "IS";
      CHECK(IS_LITERAL(ast.GetLeft())) << "IS";

      const sqlast::ColumnExpression *col = TO_COLUMN(ast.GetLeft());
      const sqlast::LiteralExpression *val = TO_LITERAL(ast.GetRight());
      size_t col_idx = this->schema_.IndexOf(col->column());
      std::vector<std::string> &vec = this->before_[col_idx];
      CHECK_EQ(vec.size(), 0u) << "Too many constraints on " << col->column();
      vec.push_back(val->value());
      break;
    }
    case sqlast::Expression::Type::IN: {
      CHECK(IS_COLUMN(ast.GetLeft())) << "IN";
      CHECK(IS_LIST(ast.GetLeft())) << "IN";

      const sqlast::ColumnExpression *col = TO_COLUMN(ast.GetLeft());
      const sqlast::LiteralListExpression *list = TO_LIST(ast.GetRight());
      size_t col_idx = this->schema_.IndexOf(col->column());
      CHECK_EQ(this->before_.count(col_idx), 0u)
          << "Too many constraints on " << col->column();
      this->before_.emplace(col_idx, list->values());
      break;
    }
    case sqlast::Expression::Type::AND: {
      this->VisitBinaryExpression(*TO_BINARY(ast.GetLeft()));
      this->VisitBinaryExpression(*TO_BINARY(ast.GetRight()));
      break;
    }
    default:  // >, LITERAL, CLUMN, LIST.
      LOG(FATAL) << "Bad expression";
  }
}

// These will never be invoked.
void ValueMapper::VisitColumnExpression(const sqlast::ColumnExpression &ast) {
  LOG(FATAL) << "UNREACHABLE";
}
void ValueMapper::VisitLiteralExpression(const sqlast::LiteralExpression &ast) {
  LOG(FATAL) << "UNREACHABLE";
}
void ValueMapper::VisitLiteralListExpression(
    const sqlast::LiteralListExpression &ast) {
  LOG(FATAL) << "UNREACHABLE";
}

}  // namespace sqlast
}  // namespace pelton
