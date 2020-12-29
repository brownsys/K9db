// Helper visitors for traversing and mutating ASTs.
#include "shards/sqlast/ast_visitors.h"

#include <cassert>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace shards {
namespace sqlast {

// Stringifier.
std::string Stringifier::VisitCreateTable(const CreateTable &ast) {
  std::string result = "CREATE TABLE " + ast.table_name() + " (";
  bool first = true;
  for (const std::string &col : ast.VisitChildren(this)) {
    if (!first) {
      result += ", ";
    }
    first = false;
    result += col;
  }
  result += ");";
  return result;
}
std::string Stringifier::VisitColumnDefinition(const ColumnDefinition &ast) {
  std::string result = ast.column_name() + " " + ast.column_type();
  for (const std::string &col : ast.VisitChildren(this)) {
    result += " " + col;
  }
  return result;
}
std::string Stringifier::VisitColumnConstraint(const ColumnConstraint &ast) {
  switch (ast.type()) {
    case ColumnConstraint::Type::PRIMARY_KEY:
      return "PRIMARY KEY";
    case ColumnConstraint::Type::UNIQUE:
      return "UNIQUE";
    case ColumnConstraint::Type::NOT_NULL:
      return "NOT NULL";
    default:
      return "REFERENCES " + ast.foreign_table() + "(" + ast.foreign_column() +
             ")";
  }
}

std::string Stringifier::VisitInsert(const Insert &ast) {
  std::string result = "INSERT INTO " + ast.table_name();
  // Columns if explicitly specified.
  if (ast.GetColumns().size() > 0) {
    result += "(";
    bool first = true;
    for (const std::string &col : ast.GetColumns()) {
      if (!first) {
        result += ", ";
      }
      first = false;
      result += col;
    }
    result += ")";
  }
  // Comma separated values.
  result += " VALUES (";
  bool first = true;
  for (const std::string &val : ast.GetValues()) {
    if (!first) {
      result += ", ";
    }
    first = false;
    result += val;
  }
  result += ");";
  return result;
}
std::string Stringifier::VisitSelect(const Select &ast) {
  std::string result = "SELECT ";
  bool first = true;
  for (const std::string &col : ast.GetColumns()) {
    if (!first) {
      result += ", ";
    }
    first = false;
    result += col;
  }
  result += " FROM " + ast.table_name();
  if (ast.HasWhereClause()) {
    result += " WHERE " + ast.VisitChildren(this).at(0);
  }
  result += ";";
  return result;
}
std::string Stringifier::VisitDelete(const Delete &ast) {
  std::string result = "DELETE FROM " + ast.table_name();
  if (ast.HasWhereClause()) {
    result += " WHERE " + ast.VisitChildren(this).at(0);
  }
  result += ";";
  return result;
}

std::string Stringifier::VisitColumnExpression(const ColumnExpression &ast) {
  return ast.column();
}
std::string Stringifier::VisitLiteralExpression(const LiteralExpression &ast) {
  return ast.value();
}
std::string Stringifier::VisitBinaryExpression(const BinaryExpression &ast) {
  std::string op = "";
  switch (ast.type()) {
    case Expression::Type::AND:
      op = " AND ";
      break;
    case Expression::Type::EQ:
      op = " = ";
      break;
    default:
      assert(false);
  }
  std::vector<std::string> children = ast.VisitChildren(this);
  return children.at(0) + op + children.at(1);
}

// ValueFinder.
std::pair<bool, std::string> ValueFinder::VisitCreateTable(
    const CreateTable &ast) {
  assert(false);
}
std::pair<bool, std::string> ValueFinder::VisitColumnDefinition(
    const ColumnDefinition &ast) {
  assert(false);
}
std::pair<bool, std::string> ValueFinder::VisitColumnConstraint(
    const ColumnConstraint &ast) {
  assert(false);
}
std::pair<bool, std::string> ValueFinder::VisitInsert(const Insert &ast) {
  assert(false);
}

std::pair<bool, std::string> ValueFinder::VisitSelect(const Select &ast) {
  auto result = ast.VisitChildren(this);
  if (result.size() == 1) {
    return result.at(0);
  }
  return std::make_pair(false, "");
}
std::pair<bool, std::string> ValueFinder::VisitDelete(const Delete &ast) {
  auto result = ast.VisitChildren(this);
  if (result.size() == 1) {
    return result.at(0);
  }
  return std::make_pair(false, "");
}

std::pair<bool, std::string> ValueFinder::VisitColumnExpression(
    const ColumnExpression &ast) {
  return std::make_pair(ast.column() == this->colname_, "");
}
std::pair<bool, std::string> ValueFinder::VisitLiteralExpression(
    const LiteralExpression &ast) {
  return std::make_pair(false, ast.value());
}
std::pair<bool, std::string> ValueFinder::VisitBinaryExpression(
    const BinaryExpression &ast) {
  auto result = ast.VisitChildren(this);
  switch (ast.type()) {
    case Expression::Type::EQ:
      if (ast.GetLeft()->type() == Expression::Type::COLUMN) {
        return std::make_pair(result.at(0).first, result.at(1).second);
      }
      if (ast.GetRight()->type() == Expression::Type::COLUMN) {
        return std::make_pair(result.at(1).first, result.at(0).second);
      }
      return std::make_pair(false, "");

    case Expression::Type::AND:
      if (result.at(0).first) {
        return result.at(0);
      }
      if (result.at(1).first) {
        return result.at(1);
      }
      return std::make_pair(false, "");
    default:
      assert(false);
  }
}

// ExpressionRemover.
std::unique_ptr<Expression> ExpressionRemover::VisitCreateTable(
    CreateTable *ast) {
  assert(false);
}
std::unique_ptr<Expression> ExpressionRemover::VisitColumnDefinition(
    ColumnDefinition *ast) {
  assert(false);
}
std::unique_ptr<Expression> ExpressionRemover::VisitColumnConstraint(
    ColumnConstraint *ast) {
  assert(false);
}
std::unique_ptr<Expression> ExpressionRemover::VisitInsert(Insert *ast) {
  assert(false);
}

std::unique_ptr<Expression> ExpressionRemover::VisitSelect(Select *ast) {
  auto result = ast->VisitChildren(this);
  if (result.size() == 1) {
    if (result.at(0).get() == nullptr) {
      ast->RemoveWhereClause();
    } else {
      std::unique_ptr<BinaryExpression> cast =
          std::unique_ptr<BinaryExpression>(
              static_cast<BinaryExpression *>(result.at(0).release()));
      ast->SetWhereClause(std::move(cast));
    }
  }
  return nullptr;
}
std::unique_ptr<Expression> ExpressionRemover::VisitDelete(Delete *ast) {
  auto result = ast->VisitChildren(this);
  if (result.size() == 1) {
    if (result.at(0).get() == nullptr) {
      ast->RemoveWhereClause();
    } else {
      std::unique_ptr<BinaryExpression> cast =
          std::unique_ptr<BinaryExpression>(
              static_cast<BinaryExpression *>(result.at(0).release()));
      ast->SetWhereClause(std::move(cast));
    }
  }
  return nullptr;
}

std::unique_ptr<Expression> ExpressionRemover::VisitColumnExpression(
    ColumnExpression *ast) {
  if (ast->column() == this->colname_) {
    return nullptr;
  }
  return ast->Clone();
}
std::unique_ptr<Expression> ExpressionRemover::VisitLiteralExpression(
    LiteralExpression *ast) {
  return ast->Clone();
}
std::unique_ptr<Expression> ExpressionRemover::VisitBinaryExpression(
    BinaryExpression *ast) {
  auto result = ast->VisitChildren(this);
  switch (ast->type()) {
    case Expression::Type::EQ:
      if (result.at(0).get() == nullptr || result.at(1).get() == nullptr) {
        return nullptr;
      }
      return ast->Clone();
    case Expression::Type::AND:
      if (result.at(0).get() == nullptr) {
        return std::move(result.at(1));
      }
      if (result.at(1).get() == nullptr) {
        return std::move(result.at(0));
      }
      return ast->Clone();
    default:
      assert(false);
  }
}

}  // namespace sqlast
}  // namespace shards
