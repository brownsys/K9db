// Helper visitors for traversing and mutating ASTs.
#include "pelton/sqlast/ast_visitors.h"

#include <cassert>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace pelton {
namespace sqlast {

/*
 * Table creation.
 */

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
  return ColumnConstraint::TypeToString(ast.type());
}

/*
 * Index and view creation.
 */

std::string Stringifier::VisitCreateIndex(const CreateIndex &ast) {
  return "CREATE INDEX " + ast.index_name() + " ON " + ast.table_name() + "(" +
         ast.column_name() + ")";
}

std::string Stringifier::VisitCreateView(const CreateView &ast) {
  return "CREATE VIEW " + ast.view_name() + " AS '\"" + ast.query() + "\"'";
}

/*
 * SQL Statements.
 */

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

std::string Stringifier::VisitReplace(const Replace &ast) {
  std::string result = "REPLACE INTO " + ast.table_name();
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

std::string Stringifier::VisitGDPRStatement(const GDPRStatement &ast) {
  std::string result = "GDPR ";
  result += ast.operation() == GDPRStatement::Operation::GET ? "GET" : "FORGET";
  result += " " + ast.shard_kind() + " " + ast.user_id().AsSQLString();
  return result;
}

/*
 * Where clause expressions.
 */

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

/* Logging abstract statements stringifies them. */
std::ostream &operator<<(std::ostream &os, const AbstractStatement &r) {
  Stringifier stringifier;
  os << stringifier.Visit(&r);
  return os;
}

}  // namespace sqlast
}  // namespace pelton
