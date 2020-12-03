// Turn an ANTLR parse tree into a valid SQL string.

#include "shards/sqlengine/visitors/stringify.h"

namespace shards {
namespace sqlengine {
namespace visitors {

// Visit an sql statement!
antlrcpp::Any Stringify::visitSql_stmt(
    sqlparser::SQLiteParser::Sql_stmtContext *ctx) {
  return ctx->create_table_stmt()->accept(this).as<std::string>() + ";";
}

// Create table.
antlrcpp::Any Stringify::visitCreate_table_stmt(
    sqlparser::SQLiteParser::Create_table_stmtContext *ctx) {
  std::string str =
      "CREATE TABLE " + ctx->table_name()->accept(this).as<std::string>();
  str += "(";
  str += ctx->column_def(0)->accept(this).as<std::string>();
  for (size_t i = 1; i < ctx->column_def().size(); i++) {
    str += ", " + ctx->column_def(i)->accept(this).as<std::string>();
  }
  for (size_t i = 0; i < ctx->table_constraint().size(); i++) {
    str += ", " + ctx->table_constraint(i)->accept(this).as<std::string>();
  }
  str += ")";
  return str;
}

antlrcpp::Any Stringify::visitColumn_def(
    sqlparser::SQLiteParser::Column_defContext *ctx) {
  std::string str = ctx->column_name()->accept(this).as<std::string>();
  if (ctx->type_name() != nullptr) {
    str += " " + ctx->type_name()->accept(this).as<std::string>();
  }
  for (size_t i = 0; i < ctx->column_constraint().size(); i++) {
    str += " ";
    if (i > 0) {
      str += ",";
    }
    str += ctx->column_constraint(i)->accept(this).as<std::string>();
  }
  return str;
}

antlrcpp::Any Stringify::visitColumn_constraint(
    sqlparser::SQLiteParser::Column_constraintContext *ctx) {
  std::string str = "";
  if (ctx->CONSTRAINT() != nullptr) {
    str += "CONSTRAINT " + ctx->name()->accept(this).as<std::string>() + " ";
  }
  if (ctx->PRIMARY() != nullptr) {
    str += "PRIMARY KEY";
  } else if (ctx->NULL_() != nullptr) {
    str += "NOT NULL";
  } else if (ctx->UNIQUE() != nullptr) {
    str += "UNIQUE";
  } else if (ctx->foreign_key_clause() != nullptr) {
    str += ctx->foreign_key_clause()->accept(this).as<std::string>();
  }
  return str;
}

antlrcpp::Any Stringify::visitTable_constraint(
    sqlparser::SQLiteParser::Table_constraintContext *ctx) {
  std::string str = "";
  if (ctx->CONSTRAINT() != nullptr) {
    str += "CONSTRAINT " + ctx->name()->accept(this).as<std::string>() + " ";
  }
  if (ctx->PRIMARY() != nullptr || ctx->UNIQUE() != nullptr) {
    str += ctx->PRIMARY() != nullptr ? "PRIMARY KEY" : "UNIQUE";
    str += "(" + ctx->indexed_column(0)->accept(this).as<std::string>();
    for (size_t i = 1; i < ctx->indexed_column().size(); i++) {
      str += ", " + ctx->indexed_column(i)->accept(this).as<std::string>();
    }
    str += ")";
  }
  if (ctx->FOREIGN() != nullptr) {
    str += "FOREIGN KEY(" + ctx->column_name(0)->accept(this).as<std::string>();
    for (size_t i = 1; i < ctx->column_name().size(); i++) {
      str += ", " + ctx->column_name(i)->accept(this).as<std::string>();
    }
    str += ") ";
    str += ctx->foreign_key_clause()->accept(this).as<std::string>();
  }
  return str;
}

// "REFERENCES <table>(<column>, ...)"
antlrcpp::Any Stringify::visitForeign_key_clause(
    sqlparser::SQLiteParser::Foreign_key_clauseContext *ctx) {
  std::string str =
      "REFERENCES " + ctx->foreign_table()->accept(this).as<std::string>();
  str += "(";
  str += ctx->column_name(0)->accept(this).as<std::string>();
  for (size_t i = 1; i < ctx->column_name().size(); i++) {
    str += ", " + ctx->column_name(i)->accept(this).as<std::string>();
  }
  str += ")";
  return str;
}

// Insert.
antlrcpp::Any Stringify::visitInsert_stmt(
    sqlparser::SQLiteParser::Insert_stmtContext *ctx) {
  std::string str = "INSERT INTO ";
  // table name and columns.
  str += ctx->table_name()->accept(this).as<std::string>();
  if (ctx->column_name().size() > 0) {
    str += "(";
    str += ctx->column_name(0)->accept(this).as<std::string>();
    for (size_t i = 1; i < ctx->column_name().size(); i++) {
      str += ", " + ctx->column_name(i)->accept(this).as<std::string>();
    }
    str += ")";
  }
  // values.
  str += " VALUES ";
  for (size_t i = 0; i < ctx->expr_list().size(); i++) {
    if (i > 0) {
      str += ", ";
    }
    str += ctx->expr_list(i)->accept(this).as<std::string>();
  }
  return str;
}

antlrcpp::Any Stringify::visitExpr_list(
    sqlparser::SQLiteParser::Expr_listContext *ctx) {
  std::string str = "(";
  for (size_t i = 0; i < ctx->expr().size(); i++) {
    if (i > 0) {
      str += ", ";
    }
    str += ctx->expr(i)->accept(this).as<std::string>();
  }
  str += ")";
  return str;
}

antlrcpp::Any Stringify::visitExpr(sqlparser::SQLiteParser::ExprContext *ctx) {
  if (ctx->literal_value() != nullptr) {
    return ctx->literal_value()->accept(this).as<std::string>();
  } else if (ctx->column_name() != nullptr) {
    return ctx->column_name()->accept(this).as<std::string>();
  } else if (ctx->ASSIGN() != nullptr) {
    return "(" + ctx->expr()[0]->accept(this).as<std::string>() + ") = (" +
           ctx->expr()[1]->accept(this).as<std::string>() + ")";
  } else if (ctx->AND() != nullptr) {
    return "(" + ctx->expr()[0]->accept(this).as<std::string>() + ") AND (" +
           ctx->expr()[1]->accept(this).as<std::string>() + ")";
  }
  throw "Unreachable code!";
}

// Select constructs.
antlrcpp::Any Stringify::visitSelect_stmt(
    sqlparser::SQLiteParser::Select_stmtContext *ctx) {
  std::string str = ctx->select_core().at(0)->accept(this).as<std::string>();
  str += ";";
  return str;
}

antlrcpp::Any Stringify::visitSelect_core(
    sqlparser::SQLiteParser::Select_coreContext *ctx) {
  std::string str = "SELECT ";
  str += ctx->result_column(0)->accept(this).as<std::string>();
  str += " FROM ";
  str += ctx->table_or_subquery().at(0)->accept(this).as<std::string>();
  if (ctx->WHERE() != nullptr && ctx->expr().size() > 0 &&
      ctx->expr(0) != nullptr) {
    str += " WHERE " + ctx->expr(0)->accept(this).as<std::string>();
  }
  return str;
}

antlrcpp::Any Stringify::visitResult_column(
    sqlparser::SQLiteParser::Result_columnContext *ctx) {
  return ctx->getText();
}

antlrcpp::Any Stringify::visitTable_or_subquery(
    sqlparser::SQLiteParser::Table_or_subqueryContext *ctx) {
  return ctx->table_name()->accept(this).as<std::string>();
}

// Building blocks.
antlrcpp::Any Stringify::visitIndexed_column(
    sqlparser::SQLiteParser::Indexed_columnContext *ctx) {
  return ctx->column_name()->getText();
}

antlrcpp::Any Stringify::visitAsc_desc(
    sqlparser::SQLiteParser::Asc_descContext *ctx) {
  return ctx->getText();
}

// Names.
antlrcpp::Any Stringify::visitName(sqlparser::SQLiteParser::NameContext *ctx) {
  return ctx->getText();
}

antlrcpp::Any Stringify::visitSchema_name(
    sqlparser::SQLiteParser::Schema_nameContext *ctx) {
  return ctx->getText();
}

antlrcpp::Any Stringify::visitTable_name(
    sqlparser::SQLiteParser::Table_nameContext *ctx) {
  return ctx->getText();
}

antlrcpp::Any Stringify::visitForeign_table(
    sqlparser::SQLiteParser::Foreign_tableContext *ctx) {
  return ctx->getText();
}

antlrcpp::Any Stringify::visitColumn_name(
    sqlparser::SQLiteParser::Column_nameContext *ctx) {
  return ctx->getText();
}

antlrcpp::Any Stringify::visitType_name(
    sqlparser::SQLiteParser::Type_nameContext *ctx) {
  return ctx->getText();
}

antlrcpp::Any Stringify::visitAny_name(
    sqlparser::SQLiteParser::Any_nameContext *ctx) {
  return ctx->getText();
}

// Values.
antlrcpp::Any Stringify::visitLiteral_value(
    sqlparser::SQLiteParser::Literal_valueContext *ctx) {
  size_t n = ctx->children.size();
  for (size_t i = 0; i < n; i++) {
    if (ctx->children[i] != nullptr) {
      return ctx->children[i]->getText();
    }
  }
  return "";
}

antlrcpp::Any Stringify::visitSigned_number(
    sqlparser::SQLiteParser::Signed_numberContext *ctx) {
  return ctx->getText();
}

// Unreachable code...
antlrcpp::Any Stringify::defaultResult() { throw "Unreachable code!"; }
antlrcpp::Any Stringify::visitTerminal(antlr4::tree::TerminalNode *node) {
  throw "Unreachable code!";
}
antlrcpp::Any Stringify::aggregateResult(antlrcpp::Any agg,
                                         const antlrcpp::Any &next) {
  throw "Unreachable code!";
}

}  // namespace visitors
}  // namespace sqlengine
}  // namespace shards
