// A default ANTLR visitor that only visist explicitly supported
// syntax and returns an error if unsupproted syntax was used.

#include "pelton/sqlast/transformer.h"

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/status/status.h"

namespace pelton {
namespace sqlast {

#define _COMMA ,

#define __CAST_OR_RETURN_VAR_NAME(arg) __CAST_OR_RETURN_RESULT_##arg
#define __CAST_OR_RETURN_VAL(arg) __CAST_OR_RETURN_VAR_NAME(arg)
#define CAST_OR_RETURN(lexpr, rexpr, type)               \
  antlrcpp::Any __CAST_OR_RETURN_VAL(__LINE__) = rexpr;  \
  if (__CAST_OR_RETURN_VAL(__LINE__).is<absl::Status>()) \
    return __CAST_OR_RETURN_VAL(__LINE__);               \
  lexpr = __CAST_OR_RETURN_VAL(__LINE__).as<type>()

#define MCAST_OR_RETURN(lexpr, rexpr, type)                        \
  antlrcpp::Any __CAST_OR_RETURN_VAL(__LINE__) = std::move(rexpr); \
  if (__CAST_OR_RETURN_VAL(__LINE__).is<absl::Status>())           \
    return __CAST_OR_RETURN_VAL(__LINE__);                         \
  lexpr = std::move(__CAST_OR_RETURN_VAL(__LINE__).as<type>())

absl::StatusOr<std::unique_ptr<AbstractStatement>>
AstTransformer::TransformStatement(
    sqlparser::SQLiteParser::Sql_stmtContext *context) {
  antlrcpp::Any result = context->accept(this);
  if (result.is<absl::Status>()) {
    return result.as<absl::Status>();
  }
  std::unique_ptr<AbstractStatement> ptr =
      std::move(result.as<std::unique_ptr<AbstractStatement>>());
  return ptr;
}

// Valid Syntax.
antlrcpp::Any AstTransformer::visitSql_stmt(
    sqlparser::SQLiteParser::Sql_stmtContext *ctx) {
  if (ctx->create_table_stmt() != nullptr) {
    return ctx->create_table_stmt()->accept(this);
  }
  if (ctx->insert_stmt() != nullptr) {
    return ctx->insert_stmt()->accept(this);
  }
  if (ctx->update_stmt() != nullptr) {
    return ctx->update_stmt()->accept(this);
  }
  if (ctx->select_stmt() != nullptr) {
    return ctx->select_stmt()->accept(this);
  }
  if (ctx->delete_stmt() != nullptr) {
    return ctx->delete_stmt()->accept(this);
  }
  if (ctx->create_view_stmt() != nullptr) {
    return ctx->create_view_stmt()->accept(this);
  }

  return absl::InvalidArgumentError("Unsupported statement type");
}

// Create a view.
antlrcpp::Any AstTransformer::visitCreate_view_stmt(
    sqlparser::SQLiteParser::Create_view_stmtContext *ctx) {
  if (ctx->TEMP() != nullptr || ctx->TEMPORARY() != nullptr ||
      ctx->EXISTS() != nullptr || ctx->schema_name() != nullptr ||
      ctx->column_name().size() != 0) {
    return absl::InvalidArgumentError("Unsupported constructs in create view");
  }

  // Parse view name.
  CAST_OR_RETURN(std::string view_name, ctx->view_name()->accept(this),
                 std::string);

  // Select statement is not parsed and treated as a literal, the planner
  // will parse and handle it.
  std::string query = ctx->QUERY_LITERAL()->getText();
  query = query.substr(2);
  query = query.substr(0, query.size() - 2);

  return static_cast<std::unique_ptr<AbstractStatement>>(
      std::make_unique<CreateView>(view_name, query));
}

// Create Table constructs.
antlrcpp::Any AstTransformer::visitCreate_table_stmt(
    sqlparser::SQLiteParser::Create_table_stmtContext *ctx) {
  if (ctx->TEMP() != nullptr || ctx->TEMPORARY() != nullptr ||
      ctx->EXISTS() != nullptr || ctx->schema_name() != nullptr ||
      ctx->WITHOUT() != nullptr || ctx->AS() != nullptr) {
    return absl::InvalidArgumentError("Unsupported constructs in create table");
  }

  // Parse table name.
  CAST_OR_RETURN(std::string table_name, ctx->table_name()->accept(this),
                 std::string);

  // Parse into our AST.
  std::unique_ptr<CreateTable> table =
      std::make_unique<CreateTable>(table_name);
  for (auto *column : ctx->column_def()) {
    CAST_OR_RETURN(ColumnDefinition col, column->accept(this),
                   ColumnDefinition);
    table->AddColumn(col.column_name(), col);
  }
  for (auto *constraint : ctx->table_constraint()) {
    CAST_OR_RETURN(const auto &[col_name _COMMA ast_constraint],
                   constraint->accept(this),
                   std::pair<std::string _COMMA ColumnConstraint>);
    if (!table->HasColumn(col_name)) {
      return absl::InvalidArgumentError("Constraint over non-existing column");
    }
    table->MutableColumn(col_name).AddConstraint(ast_constraint);
  }

  // Upcast to an abstract statement.
  return static_cast<std::unique_ptr<AbstractStatement>>(std::move(table));
}

antlrcpp::Any AstTransformer::visitColumn_def(
    sqlparser::SQLiteParser::Column_defContext *ctx) {
  // Visit column name.
  CAST_OR_RETURN(std::string column_name, ctx->column_name()->accept(this),
                 std::string);
  // Visit type (if exists).
  std::string column_type = "";
  if (ctx->type_name() != nullptr) {
    CAST_OR_RETURN(column_type, ctx->type_name()->accept(this), std::string);
  }

  // Visit inlined constraints.
  ColumnDefinition column(column_name, column_type);
  for (auto *constraint : ctx->column_constraint()) {
    CAST_OR_RETURN(ColumnConstraint constr, constraint->accept(this),
                   ColumnConstraint);
    column.AddConstraint(constr);
  }
  return column;
}

antlrcpp::Any AstTransformer::visitColumn_constraint(
    sqlparser::SQLiteParser::Column_constraintContext *ctx) {
  if (ctx->asc_desc() != nullptr || ctx->conflict_clause() != nullptr ||
      ctx->CHECK() != nullptr || ctx->DEFAULT() != nullptr ||
      ctx->COLLATE() != nullptr || ctx->AS() != nullptr ||
      ctx->CONSTRAINT() != nullptr || ctx->AUTOINCREMENT() != nullptr) {
    return absl::InvalidArgumentError("Invalid inline constraint");
  }

  if (ctx->PRIMARY() != nullptr) {
    return ColumnConstraint(ColumnConstraint::Type::PRIMARY_KEY);
  }
  if (ctx->NOT() != nullptr) {
    return ColumnConstraint(ColumnConstraint::Type::NOT_NULL);
  }
  if (ctx->UNIQUE() != nullptr) {
    return ColumnConstraint(ColumnConstraint::Type::UNIQUE);
  }
  return ctx->foreign_key_clause()->accept(this);
}

antlrcpp::Any AstTransformer::visitTable_constraint(
    sqlparser::SQLiteParser::Table_constraintContext *ctx) {
  if (ctx->CONSTRAINT() != nullptr || ctx->conflict_clause() != nullptr ||
      ctx->CHECK() != nullptr || ctx->column_name().size() > 1 ||
      ctx->indexed_column().size() > 1) {
    return absl::InvalidArgumentError("Invalid table constraint");
  }

  if (ctx->PRIMARY() != nullptr) {
    CAST_OR_RETURN(std::string col_name, ctx->indexed_column(0)->accept(this),
                   std::string);
    return std::make_pair(
        col_name, ColumnConstraint(ColumnConstraint::Type::PRIMARY_KEY));
  }
  if (ctx->UNIQUE() != nullptr) {
    CAST_OR_RETURN(std::string col_name, ctx->indexed_column(0)->accept(this),
                   std::string);
    return std::make_pair(col_name,
                          ColumnConstraint(ColumnConstraint::Type::UNIQUE));
  }
  // Can only be a foreign key.
  CAST_OR_RETURN(std::string col_name, ctx->column_name(0)->accept(this),
                 std::string);
  CAST_OR_RETURN(ColumnConstraint fk_constraint,
                 ctx->foreign_key_clause()->accept(this), ColumnConstraint);
  return std::make_pair(col_name, fk_constraint);
}

antlrcpp::Any AstTransformer::visitForeign_key_clause(
    sqlparser::SQLiteParser::Foreign_key_clauseContext *ctx) {
  if (ctx->ON().size() > 0 || ctx->MATCH().size() > 0 ||
      ctx->DEFERRABLE() != nullptr || ctx->column_name().size() != 1) {
    return absl::InvalidArgumentError("Invalid foreign key constraint");
  }

  ColumnConstraint constrn(ColumnConstraint::Type::FOREIGN_KEY);
  CAST_OR_RETURN(constrn.foreign_table(), ctx->foreign_table()->accept(this),
                 std::string);
  CAST_OR_RETURN(constrn.foreign_column(), ctx->column_name(0)->accept(this),
                 std::string);
  return constrn;
}

// Insert constructs.
antlrcpp::Any AstTransformer::visitInsert_stmt(
    sqlparser::SQLiteParser::Insert_stmtContext *ctx) {
  if (ctx->REPLACE() != nullptr || ctx->OR() != nullptr ||
      ctx->AS() != nullptr || ctx->select_stmt() != nullptr ||
      ctx->upsert_clause() != nullptr || ctx->DEFAULT() != nullptr ||
      ctx->schema_name() != nullptr) {
    return absl::InvalidArgumentError("Invalid insert constructs");
  }
  if (ctx->expr_list().size() > 1) {
    return absl::InvalidArgumentError("Can only insert one row at a time");
  }

  // Table name and explicitly specified columns.
  CAST_OR_RETURN(std::string table_name, ctx->table_name()->accept(this),
                 std::string);
  std::unique_ptr<Insert> insert = std::make_unique<Insert>(table_name);
  for (auto column_name_ctx : ctx->column_name()) {
    CAST_OR_RETURN(std::string column_name, column_name_ctx->accept(this),
                   std::string);
    insert->AddColumn(column_name);
  }

  // Inserted values.
  CAST_OR_RETURN(std::vector<std::string> values,
                 ctx->expr_list(0)->accept(this), std::vector<std::string>);
  insert->SetValues(std::move(values));
  return static_cast<std::unique_ptr<AbstractStatement>>(std::move(insert));
}
antlrcpp::Any AstTransformer::visitExpr_list(
    sqlparser::SQLiteParser::Expr_listContext *ctx) {
  std::vector<std::string> values;
  for (auto expr_ctx : ctx->expr()) {
    if (expr_ctx->literal_value() == nullptr) {
      return absl::InvalidArgumentError("Non-literal value in insert");
    }
    CAST_OR_RETURN(std::string val, expr_ctx->literal_value()->accept(this),
                   std::string);
    values.push_back(val);
  }
  return values;
}

// Update constructs.
antlrcpp::Any AstTransformer::visitUpdate_stmt(
    sqlparser::SQLiteParser::Update_stmtContext *ctx) {
  if (ctx->with_clause() != nullptr || ctx->OR() != nullptr ||
      ctx->column_name_list().size() > 0) {
    return absl::InvalidArgumentError("Invalid update constructs");
  }

  // Table name.
  CAST_OR_RETURN(std::string table_name,
                 ctx->qualified_table_name()->accept(this), std::string);
  std::unique_ptr<Update> update = std::make_unique<Update>(table_name);
  // Column = Value pairs.
  for (size_t i = 0; i < ctx->column_name().size(); i++) {
    CAST_OR_RETURN(std::string column, ctx->column_name(i)->accept(this),
                   std::string);
    MCAST_OR_RETURN(std::unique_ptr<Expression> value_expr,
                    ctx->expr(i)->accept(this), std::unique_ptr<Expression>);
    if (value_expr->type() != Expression::Type::LITERAL) {
      return absl::InvalidArgumentError("Update value must be a literal");
    }
    update->AddColumnValue(
        column, static_cast<LiteralExpression *>(value_expr.get())->value());
  }
  // Where clause.
  if (ctx->WHERE() != nullptr) {
    MCAST_OR_RETURN(std::unique_ptr<Expression> expr,
                    ctx->expr(ctx->column_name().size())->accept(this),
                    std::unique_ptr<Expression>);
    if (expr->type() != Expression::Type::EQ &&
        expr->type() != Expression::Type::AND) {
      return absl::InvalidArgumentError("Where clause must be boolean");
    }
    std::unique_ptr<BinaryExpression> bexpr(
        static_cast<BinaryExpression *>(expr.release()));
    update->SetWhereClause(std::move(bexpr));
  }
  return static_cast<std::unique_ptr<AbstractStatement>>(std::move(update));
}

// Select constructs.
antlrcpp::Any AstTransformer::visitSelect_stmt(
    sqlparser::SQLiteParser::Select_stmtContext *ctx) {
  if (ctx->common_table_stmt() != nullptr ||
      ctx->compound_operator().size() != 0 || ctx->select_core().size() != 1 ||
      ctx->order_by_stmt() != nullptr) {
    return absl::InvalidArgumentError("Invalid select construct");
  }

  // Parse select_core.
  MCAST_OR_RETURN(std::unique_ptr<AbstractStatement> select_stmt,
                  ctx->select_core(0)->accept(this),
                  std::unique_ptr<AbstractStatement>);

  // Parse OFFSET and LIMIT if they exist.
  if (ctx->limit_stmt() != nullptr) {
    CAST_OR_RETURN(auto [offset _COMMA limit], ctx->limit_stmt()->accept(this),
                   std::pair<size_t _COMMA int>);

    Select *select = static_cast<Select *>(select_stmt.get());
    select->limit() = limit;
    select->offset() = offset;
  }

  return std::move(select_stmt);
}
antlrcpp::Any AstTransformer::visitSelect_core(
    sqlparser::SQLiteParser::Select_coreContext *ctx) {
  if (ctx->DISTINCT() != nullptr || ctx->ALL() != nullptr ||
      ctx->result_column().size() != 1 ||
      ctx->table_or_subquery().size() != 1 || ctx->GROUP() != nullptr ||
      ctx->HAVING() != nullptr || ctx->WINDOW() != nullptr ||
      ctx->VALUES() != nullptr) {
    return absl::InvalidArgumentError("Unsupported select modifiers");
  }

  // Find the table name.
  CAST_OR_RETURN(std::string table_name,
                 ctx->table_or_subquery(0)->accept(this), std::string);
  std::unique_ptr<Select> select = std::make_unique<Select>(table_name);

  for (auto *result_column_ctx : ctx->result_column()) {
    CAST_OR_RETURN(std::string column, result_column_ctx->accept(this),
                   std::string);
    select->AddColumn(column);
  }

  if (ctx->WHERE() != nullptr) {
    MCAST_OR_RETURN(std::unique_ptr<Expression> expr,
                    ctx->expr(0)->accept(this), std::unique_ptr<Expression>);
    if (expr->type() != Expression::Type::EQ &&
        expr->type() != Expression::Type::AND &&
        expr->type() != Expression::Type::GREATER_THAN) {
      return absl::InvalidArgumentError("Where clause must be boolean");
    }

    std::unique_ptr<BinaryExpression> bexpr(
        static_cast<BinaryExpression *>(expr.release()));
    select->SetWhereClause(std::move(bexpr));
  }
  return static_cast<std::unique_ptr<AbstractStatement>>(std::move(select));
}
antlrcpp::Any AstTransformer::visitResult_column(
    sqlparser::SQLiteParser::Result_columnContext *ctx) {
  if (ctx->STAR() == nullptr) {
    return absl::InvalidArgumentError("Only support * in select");
  }
  return std::string("*");
}
antlrcpp::Any AstTransformer::visitTable_or_subquery(
    sqlparser::SQLiteParser::Table_or_subqueryContext *ctx) {
  if (ctx->schema_name() != nullptr || ctx->AS() != nullptr ||
      ctx->table_alias() != nullptr || ctx->INDEXED() != nullptr ||
      ctx->table_name() == nullptr) {
    return absl::InvalidArgumentError("Invalid select domain");
  }
  return ctx->table_name()->accept(this);
}
antlrcpp::Any AstTransformer::visitLimit_stmt(
    sqlparser::SQLiteParser::Limit_stmtContext *ctx) {
  std::string limit = ctx->expr().at(0)->getText();
  if (ctx->expr().size() > 1) {
    std::string offset = ctx->expr().at(1)->getText();
    return std::pair<size_t, int>(std::stoul(offset), std::stoi(limit));
  }
  return std::pair<size_t, int>(0, std::stoi(limit));
}

// Delete statement.
antlrcpp::Any AstTransformer::visitDelete_stmt(
    sqlparser::SQLiteParser::Delete_stmtContext *ctx) {
  if (ctx->with_clause() != nullptr) {
    return absl::InvalidArgumentError("Invalid delete statement");
  }

  // Find table name.
  CAST_OR_RETURN(std::string table_name,
                 ctx->qualified_table_name()->accept(this), std::string);
  std::unique_ptr<Delete> delete_ = std::make_unique<Delete>(table_name);
  // Where clause if exists.
  if (ctx->WHERE() != nullptr) {
    MCAST_OR_RETURN(std::unique_ptr<Expression> expr, ctx->expr()->accept(this),
                    std::unique_ptr<Expression>);
    if (expr->type() != Expression::Type::EQ &&
        expr->type() != Expression::Type::AND) {
      return absl::InvalidArgumentError("Where clause must be boolean");
    }

    std::unique_ptr<BinaryExpression> bexpr(
        static_cast<BinaryExpression *>(expr.release()));
    delete_->SetWhereClause(std::move(bexpr));
  }
  return static_cast<std::unique_ptr<AbstractStatement>>(std::move(delete_));
}
antlrcpp::Any AstTransformer::visitQualified_table_name(
    sqlparser::SQLiteParser::Qualified_table_nameContext *ctx) {
  if (ctx->schema_name() != nullptr || ctx->AS() != nullptr ||
      ctx->INDEXED() != nullptr || ctx->INDEXED() != nullptr) {
    return absl::InvalidArgumentError("Invalid delete domain");
  }
  return ctx->table_name()->accept(this);
}

// Common constructs.
// Expressions.
antlrcpp::Any AstTransformer::visitExpr(
    sqlparser::SQLiteParser::ExprContext *ctx) {
  if ((ctx->literal_value() == nullptr && ctx->ASSIGN() == nullptr &&
       ctx->column_name() == nullptr && ctx->AND() == nullptr &&
       ctx->GT() == nullptr) ||
      ctx->expr().size() > 2) {
    return absl::InvalidArgumentError("Unsupported expression");
  }

  if (ctx->literal_value() != nullptr) {
    CAST_OR_RETURN(std::string value, ctx->literal_value()->accept(this),
                   std::string);
    std::unique_ptr<Expression> result =
        std::make_unique<LiteralExpression>(value);
    return result;
  }
  if (ctx->column_name() != nullptr) {
    CAST_OR_RETURN(std::string column, ctx->column_name()->accept(this),
                   std::string);
    std::unique_ptr<Expression> result =
        std::make_unique<ColumnExpression>(column);
    return result;
  }

  std::unique_ptr<BinaryExpression> result;
  if (ctx->ASSIGN() != nullptr) {
    result = std::make_unique<BinaryExpression>(Expression::Type::EQ);
  }
  if (ctx->AND() != nullptr) {
    result = std::make_unique<BinaryExpression>(Expression::Type::AND);
  }
  if (ctx->GT() != nullptr) {
    result = std::make_unique<BinaryExpression>(Expression::Type::GREATER_THAN);
  }

  MCAST_OR_RETURN(std::unique_ptr<Expression> expr0, ctx->expr(0)->accept(this),
                  std::unique_ptr<Expression>);
  MCAST_OR_RETURN(std::unique_ptr<Expression> expr1, ctx->expr(1)->accept(this),
                  std::unique_ptr<Expression>);
  result->SetLeft(std::move(expr0));
  result->SetRight(std::move(expr1));
  return static_cast<std::unique_ptr<Expression>>(std::move(result));
}
antlrcpp::Any AstTransformer::visitLiteral_value(
    sqlparser::SQLiteParser::Literal_valueContext *ctx) {
  return ctx->getText();
}

// Names of entities (columns, tables, etc).
antlrcpp::Any AstTransformer::visitIndexed_column(
    sqlparser::SQLiteParser::Indexed_columnContext *ctx) {
  if (ctx->COLLATE() != nullptr || ctx->expr() != nullptr ||
      ctx->collation_name() != nullptr || ctx->asc_desc() != nullptr ||
      ctx->column_name() == nullptr) {
    return absl::InvalidArgumentError("Invalid column name");
  }
  return ctx->column_name()->accept(this);
}
antlrcpp::Any AstTransformer::visitName(
    sqlparser::SQLiteParser::NameContext *ctx) {
  return ctx->any_name()->accept(this);
}
antlrcpp::Any AstTransformer::visitTable_name(
    sqlparser::SQLiteParser::Table_nameContext *ctx) {
  return ctx->any_name()->accept(this);
}
antlrcpp::Any AstTransformer::visitView_name(
    sqlparser::SQLiteParser::View_nameContext *ctx) {
  return ctx->any_name()->accept(this);
}
antlrcpp::Any AstTransformer::visitForeign_table(
    sqlparser::SQLiteParser::Foreign_tableContext *ctx) {
  return ctx->any_name()->accept(this);
}
antlrcpp::Any AstTransformer::visitColumn_name(
    sqlparser::SQLiteParser::Column_nameContext *ctx) {
  return ctx->any_name()->accept(this);
}
antlrcpp::Any AstTransformer::visitType_name(
    sqlparser::SQLiteParser::Type_nameContext *ctx) {
  return ctx->getText();
}
antlrcpp::Any AstTransformer::visitAny_name(
    sqlparser::SQLiteParser::Any_nameContext *ctx) {
  if (ctx->IDENTIFIER() == nullptr) {
    return absl::InvalidArgumentError("All names must be simple and unquoted");
  }
  return ctx->getText();
}

// Invalid Syntax!
antlrcpp::Any AstTransformer::visitParse(
    sqlparser::SQLiteParser::ParseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitError(
    sqlparser::SQLiteParser::ErrorContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitSql_stmt_list(
    sqlparser::SQLiteParser::Sql_stmt_listContext *context) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitAlter_table_stmt(
    sqlparser::SQLiteParser::Alter_table_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitAnalyze_stmt(
    sqlparser::SQLiteParser::Analyze_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitAttach_stmt(
    sqlparser::SQLiteParser::Attach_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitBegin_stmt(
    sqlparser::SQLiteParser::Begin_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitCommit_stmt(
    sqlparser::SQLiteParser::Commit_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitRollback_stmt(
    sqlparser::SQLiteParser::Rollback_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitSavepoint_stmt(
    sqlparser::SQLiteParser::Savepoint_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitRelease_stmt(
    sqlparser::SQLiteParser::Release_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitCreate_index_stmt(
    sqlparser::SQLiteParser::Create_index_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitSigned_number(
    sqlparser::SQLiteParser::Signed_numberContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitConflict_clause(
    sqlparser::SQLiteParser::Conflict_clauseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitCreate_trigger_stmt(
    sqlparser::SQLiteParser::Create_trigger_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitCreate_virtual_table_stmt(
    sqlparser::SQLiteParser::Create_virtual_table_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitWith_clause(
    sqlparser::SQLiteParser::With_clauseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitCte_table_name(
    sqlparser::SQLiteParser::Cte_table_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitRecursive_cte(
    sqlparser::SQLiteParser::Recursive_cteContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitCommon_table_expression(
    sqlparser::SQLiteParser::Common_table_expressionContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitDelete_stmt_limited(
    sqlparser::SQLiteParser::Delete_stmt_limitedContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitDetach_stmt(
    sqlparser::SQLiteParser::Detach_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitDrop_stmt(
    sqlparser::SQLiteParser::Drop_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitRaise_function(
    sqlparser::SQLiteParser::Raise_functionContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitUpsert_clause(
    sqlparser::SQLiteParser::Upsert_clauseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitPragma_stmt(
    sqlparser::SQLiteParser::Pragma_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitPragma_value(
    sqlparser::SQLiteParser::Pragma_valueContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitReindex_stmt(
    sqlparser::SQLiteParser::Reindex_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitJoin_clause(
    sqlparser::SQLiteParser::Join_clauseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitFactored_select_stmt(
    sqlparser::SQLiteParser::Factored_select_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitSimple_select_stmt(
    sqlparser::SQLiteParser::Simple_select_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitCompound_select_stmt(
    sqlparser::SQLiteParser::Compound_select_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitJoin_operator(
    sqlparser::SQLiteParser::Join_operatorContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitJoin_constraint(
    sqlparser::SQLiteParser::Join_constraintContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitCompound_operator(
    sqlparser::SQLiteParser::Compound_operatorContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitColumn_name_list(
    sqlparser::SQLiteParser::Column_name_listContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitUpdate_stmt_limited(
    sqlparser::SQLiteParser::Update_stmt_limitedContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitVacuum_stmt(
    sqlparser::SQLiteParser::Vacuum_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitFilter_clause(
    sqlparser::SQLiteParser::Filter_clauseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitWindow_defn(
    sqlparser::SQLiteParser::Window_defnContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitOver_clause(
    sqlparser::SQLiteParser::Over_clauseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitFrame_spec(
    sqlparser::SQLiteParser::Frame_specContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitFrame_clause(
    sqlparser::SQLiteParser::Frame_clauseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitSimple_function_invocation(
    sqlparser::SQLiteParser::Simple_function_invocationContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitAggregate_function_invocation(
    sqlparser::SQLiteParser::Aggregate_function_invocationContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitWindow_function_invocation(
    sqlparser::SQLiteParser::Window_function_invocationContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitCommon_table_stmt(
    sqlparser::SQLiteParser::Common_table_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitOrder_by_stmt(
    sqlparser::SQLiteParser::Order_by_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitOrdering_term(
    sqlparser::SQLiteParser::Ordering_termContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitAsc_desc(
    sqlparser::SQLiteParser::Asc_descContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitFrame_left(
    sqlparser::SQLiteParser::Frame_leftContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitFrame_right(
    sqlparser::SQLiteParser::Frame_rightContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitFrame_single(
    sqlparser::SQLiteParser::Frame_singleContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitWindow_function(
    sqlparser::SQLiteParser::Window_functionContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitOffset(
    sqlparser::SQLiteParser::OffsetContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitDefault_value(
    sqlparser::SQLiteParser::Default_valueContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitPartition_by(
    sqlparser::SQLiteParser::Partition_byContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitOrder_by_expr(
    sqlparser::SQLiteParser::Order_by_exprContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitOrder_by_expr_asc_desc(
    sqlparser::SQLiteParser::Order_by_expr_asc_descContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitExpr_asc_desc(
    sqlparser::SQLiteParser::Expr_asc_descContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitInitial_select(
    sqlparser::SQLiteParser::Initial_selectContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitRecursive_select(
    sqlparser::SQLiteParser::Recursive_selectContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitUnary_operator(
    sqlparser::SQLiteParser::Unary_operatorContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitError_message(
    sqlparser::SQLiteParser::Error_messageContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitModule_argument(
    sqlparser::SQLiteParser::Module_argumentContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitColumn_alias(
    sqlparser::SQLiteParser::Column_aliasContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitKeyword(
    sqlparser::SQLiteParser::KeywordContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitSchema_name(
    sqlparser::SQLiteParser::Schema_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitFunction_name(
    sqlparser::SQLiteParser::Function_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitTable_or_index_name(
    sqlparser::SQLiteParser::Table_or_index_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitNew_table_name(
    sqlparser::SQLiteParser::New_table_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitCollation_name(
    sqlparser::SQLiteParser::Collation_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitIndex_name(
    sqlparser::SQLiteParser::Index_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitTrigger_name(
    sqlparser::SQLiteParser::Trigger_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitModule_name(
    sqlparser::SQLiteParser::Module_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitPragma_name(
    sqlparser::SQLiteParser::Pragma_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitSavepoint_name(
    sqlparser::SQLiteParser::Savepoint_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitTable_alias(
    sqlparser::SQLiteParser::Table_aliasContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitTransaction_name(
    sqlparser::SQLiteParser::Transaction_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitWindow_name(
    sqlparser::SQLiteParser::Window_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitAlias(
    sqlparser::SQLiteParser::AliasContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitFilename(
    sqlparser::SQLiteParser::FilenameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitBase_window_name(
    sqlparser::SQLiteParser::Base_window_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitSimple_func(
    sqlparser::SQLiteParser::Simple_funcContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitAggregate_func(
    sqlparser::SQLiteParser::Aggregate_funcContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any AstTransformer::visitTable_function_name(
    sqlparser::SQLiteParser::Table_function_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}

}  // namespace sqlast
}  // namespace pelton
