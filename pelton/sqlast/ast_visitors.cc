// Helper visitors for traversing and mutating ASTs.
#include "pelton/sqlast/ast_visitors.h"

#include <cassert>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "pelton/util/perf.h"

namespace pelton {
namespace sqlast {

namespace {

std::string Dequote(const std::string &st) {
  std::string s(st);
  s.erase(remove(s.begin(), s.end(), '\"'), s.end());
  s.erase(remove(s.begin(), s.end(), '\''), s.end());
  return s;
}

}  // namespace

// Stringifier.
std::string Stringifier::VisitCreateTable(const CreateTable &ast) {
  perf::Start("Stringify (create)");
  std::string result =
      "CREATE TABLE " + this->shard_prefix_ + ast.table_name() + " (";
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
  perf::End("Stringify (create)");
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
      if (this->supports_foreign_keys_) {
        return "REFERENCES " + this->shard_prefix_ + ast.foreign_table() + "(" +
               ast.foreign_column() + ")";
      } else {
        return "";
      }
    default:
      assert(false);
  }
}

std::string Stringifier::VisitCreateIndex(const CreateIndex &ast) {
  perf::Start("Stringify (create index)");
  std::string result = "CREATE INDEX " + this->shard_prefix_ +
                       ast.index_name() + " ON " + this->shard_prefix_ +
                       ast.table_name() + "(" + ast.column_name() + ")";
  perf::End("Stringify (create index)");
  return result;
}

std::string Stringifier::VisitInsert(const Insert &ast) {
  perf::Start("Stringify (insert)");
  std::string result = ast.replace() ? "REPLACE" : "INSERT";
  result += " INTO " + this->shard_prefix_ + ast.table_name();
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
  result += ")";
  perf::End("Stringify (insert)");
  return result;
}
std::string Stringifier::VisitUpdate(const Update &ast) {
  perf::Start("Stringify (update)");
  std::string result =
      "UPDATE " + this->shard_prefix_ + ast.table_name() + " SET ";
  // Columns and values.
  const std::vector<std::string> &cols = ast.GetColumns();
  const std::vector<std::string> &vals = ast.GetValues();
  for (size_t i = 0; i < cols.size(); i++) {
    result += cols.at(i) + " = " + vals.at(i);
    if (i < cols.size() - 1) {
      result += ", ";
    }
  }
  if (ast.HasWhereClause()) {
    result += " WHERE " + ast.VisitChildren(this).at(0);
  }
  perf::End("Stringify (update)");
  return result;
}
std::string Stringifier::VisitSelect(const Select &ast) {
  perf::Start("Stringify (select)");
  std::string result = "SELECT ";
  bool first = true;
  for (const std::string &col : ast.GetColumns()) {
    if (!first) {
      result += ", ";
    }
    first = false;
    result += col;
  }
  result += " FROM " + this->shard_prefix_ + ast.table_name();
  if (ast.HasWhereClause()) {
    result += " WHERE " + ast.VisitChildren(this).at(0);
  }
  perf::End("Stringify (select)");
  return result;
}
std::string Stringifier::VisitDelete(const Delete &ast) {
  perf::Start("Stringify (delete)");
  std::string result = "DELETE FROM " + this->shard_prefix_ + ast.table_name();
  if (ast.HasWhereClause()) {
    result += " WHERE " + ast.VisitChildren(this).at(0);
  }
  if (ast.returning()) {
    result += " RETURNING *";
  }
  perf::End("Stringify (delete)");
  return result;
}

std::string Stringifier::VisitColumnExpression(const ColumnExpression &ast) {
  return ast.column();
}
std::string Stringifier::VisitLiteralExpression(const LiteralExpression &ast) {
  return ast.value();
}
std::string Stringifier::VisitLiteralListExpression(
    const LiteralListExpression &ast) {
  std::string list = "(";
  for (const std::string &val : ast.values()) {
    list += list.size() == 1 ? val : (", " + val);
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
std::pair<bool, std::string> ValueFinder::VisitCreateIndex(
    const CreateIndex &ast) {
  assert(false);
}
std::pair<bool, std::string> ValueFinder::VisitInsert(const Insert &ast) {
  assert(false);
}

std::pair<bool, std::string> ValueFinder::VisitUpdate(const Update &ast) {
  auto result = ast.VisitChildren(this);
  if (result.size() == 1) {
    return result.at(0);
  }
  return std::make_pair(false, "");
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
  return std::make_pair(false, Dequote(ast.value()));
}
std::pair<bool, std::string> ValueFinder::VisitLiteralListExpression(
    const LiteralListExpression &ast) {
  if (ast.values().size() == 1) {
    return std::make_pair(true, Dequote(ast.values().at(0)));
  }
  return std::make_pair(false, "");
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

    case Expression::Type::IS:
      // IS cannot be used on a shard by column since it is not nullable.
      if (result.at(0).first) {
        assert(false);
      }
      return std::make_pair(false, "");

    case Expression::Type::IN:
      // IN can be used on a shard by column when the value list is a singleton.
      if (result.at(0).first && !result.at(1).first) {
        assert(false);
      }
      return std::make_pair(result.at(0).first, result.at(1).second);

    case Expression::Type::AND:
      if (result.at(0).first) {
        return result.at(0);
      }
      if (result.at(1).first) {
        return result.at(1);
      }
      return std::make_pair(false, "");

    case Expression::Type::GREATER_THAN:
      // GREATER_THAN cannot be used on a shard by column.
      if (result.at(0).first) {
        assert(false);
      } else if (result.at(1).first) {
        assert(false);
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
std::unique_ptr<Expression> ExpressionRemover::VisitCreateIndex(
    CreateIndex *ast) {
  assert(false);
}
std::unique_ptr<Expression> ExpressionRemover::VisitInsert(Insert *ast) {
  assert(false);
}

std::unique_ptr<Expression> ExpressionRemover::VisitUpdate(Update *ast) {
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
std::unique_ptr<Expression> ExpressionRemover::VisitLiteralListExpression(
    LiteralListExpression *ast) {
  return ast->Clone();
}
std::unique_ptr<Expression> ExpressionRemover::VisitBinaryExpression(
    BinaryExpression *ast) {
  auto result = ast->VisitChildren(this);
  switch (ast->type()) {
    case Expression::Type::EQ:
    case Expression::Type::IS:
    case Expression::Type::IN:
      if (result.at(0).get() == nullptr || result.at(1).get() == nullptr) {
        return nullptr;
      }
      return ast->Clone();
    case Expression::Type::AND: {
      std::unique_ptr<Expression> &left = result.at(0);
      std::unique_ptr<Expression> &right = result.at(1);
      if (left.get() == nullptr && right.get() == nullptr) {
        return nullptr;
      }
      if (left.get() == nullptr) {
        return std::move(right);
      }
      if (right.get() == nullptr) {
        return std::move(left);
      }

      std::unique_ptr<BinaryExpression> binary =
          std::make_unique<BinaryExpression>(Expression::Type::AND);
      binary->SetLeft(std::move(left));
      binary->SetRight(std::move(right));
      return std::move(binary);
    }
    default:
      assert(false);
  }
}

std::ostream &operator<<(std::ostream &os, const AbstractStatement &r) {
  Stringifier stringifier("");
  os << stringifier.Visit(&r);
  return os;
}

}  // namespace sqlast
}  // namespace pelton
