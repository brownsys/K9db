// Helper visitors for traversing and mutating ASTs.
#include "pelton/sqlast/ast_visitors.h"

#include <cassert>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace pelton {
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
  result += ")";
  result += " ENGINE ROCKSDB";
  return result;
}
std::string Stringifier::VisitColumnDefinition(const ColumnDefinition &ast) {
  std::string result = ast.column_name() + " " +
                       ColumnDefinition::TypeToString(ast.column_type());
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
    case ColumnConstraint::Type::FOREIGN_KEY:
      return "REFERENCES " + ast.foreign_table() + "(" + ast.foreign_column() +
             ")";
    case ColumnConstraint::Type::AUTOINCREMENT:
      return "AUTO_INCREMENT";
    default:
      assert(false);
  }
}

std::string Stringifier::VisitCreateIndex(const CreateIndex &ast) {
  std::string result = "CREATE INDEX " + ast.index_name() + " ON " +
                       ast.table_name() + "(" + ast.column_name() + ")";
  return result;
}

std::string Stringifier::VisitInsert(const Insert &ast) {
  std::string result = "INSERT INTO " + ast.table_name();
  // Columns if explicitly specified.
  if (ast.HasColumns()) {
    const std::vector<std::string> &columns = ast.GetColumns();
    result += "(";
    for (size_t i = 0; i < columns.size(); i++) {
      if (i > 0) {
        result += ", ";
      }
      result += columns.at(i);
    }
    result += ")";
  }
  // Comma separated values.
  const std::vector<Value> &values = ast.GetValues();
  result += " VALUES (";
  for (size_t i = 0; i < values.size(); i++) {
    if (i > 0) {
      result += ", ";
    }
    result += values.at(i).AsSQLString();
  }
  result += ")";
  return result;
}
std::string Stringifier::VisitUpdate(const Update &ast) {
  std::string result = "UPDATE " + ast.table_name() + " SET ";
  // Columns and values.
  const std::vector<std::string> &cols = ast.GetColumns();
  const std::vector<Value> &vals = ast.GetValues();
  for (size_t i = 0; i < cols.size(); i++) {
    result += cols.at(i) + " = " + vals.at(i).AsSQLString();
    if (i < cols.size() - 1) {
      result += ", ";
    }
  }
  if (ast.HasWhereClause()) {
    result += " WHERE " + ast.VisitChildren(this).at(0);
  }
  return result;
}
std::string Stringifier::VisitSelect(const Select &ast) {
  std::string result = "SELECT ";
  bool first = true;
  for (const Select::ResultColumn &col : ast.GetColumns()) {
    if (!first) {
      result += ", ";
    }
    first = false;
    result += col.ToString();
  }
  result += " FROM " + ast.table_name();
  if (ast.HasWhereClause()) {
    result += " WHERE " + ast.VisitChildren(this).at(0);
  }
  return result;
}
std::string Stringifier::VisitDelete(const Delete &ast) {
  std::string result = "DELETE FROM " + ast.table_name();
  if (ast.HasWhereClause()) {
    result += " WHERE " + ast.VisitChildren(this).at(0);
  }
  return result;
}

std::string Stringifier::VisitColumnExpression(const ColumnExpression &ast) {
  return ast.column();
}
std::string Stringifier::VisitLiteralExpression(const LiteralExpression &ast) {
  return ast.value().AsSQLString();
}
std::string Stringifier::VisitLiteralListExpression(
    const LiteralListExpression &ast) {
  std::string list = "(";
  for (const Value &val : ast.values()) {
    list += (list.size() == 1 ? "" : ", ") + val.AsSQLString();
  }
  list += ")";
  return list;
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
    case Expression::Type::IS:
      op = " IS ";
      break;
    case Expression::Type::IN:
      op = " IN ";
      break;
    case Expression::Type::GREATER_THAN:
      op = " > ";
      break;
    default:
      assert(false);
  }
  std::vector<std::string> children = ast.VisitChildren(this);
  return children.at(0) + op + children.at(1);
}

std::ostream &operator<<(std::ostream &os, const AbstractStatement &r) {
  Stringifier stringifier;
  os << stringifier.Visit(&r);
  return os;
}

}  // namespace sqlast
}  // namespace pelton
