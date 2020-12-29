// A default ANTLR visitor that only visist explicitly supported
// syntax and throws an error if unsupproted syntax was used.

#include "shards/sqlengine/visitors/ast.h"

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/status/status.h"

namespace shards {
namespace sqlengine {
namespace visitors {

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

absl::StatusOr<std::unique_ptr<sqlast::AbstractStatement>>
BuildAstVisitor::TransformStatement(
    sqlparser::SQLiteParser::Sql_stmtContext *context) {
  antlrcpp::Any result = context->accept(this);
  if (result.is<absl::Status>()) {
    return result.as<absl::Status>();
  }
  std::unique_ptr<sqlast::AbstractStatement> ptr =
      std::move(result.as<std::unique_ptr<sqlast::AbstractStatement>>());
  return ptr;
}

// Valid Syntax.
antlrcpp::Any BuildAstVisitor::visitSql_stmt(
    sqlparser::SQLiteParser::Sql_stmtContext *ctx) {
  if (ctx->create_table_stmt() != nullptr) {
    return ctx->create_table_stmt()->accept(this);
  }
  if (ctx->insert_stmt() != nullptr) {
    return ctx->insert_stmt()->accept(this);
  }
  if (ctx->select_stmt() != nullptr) {
    return ctx->select_stmt()->accept(this);
  }
  if (ctx->delete_stmt() != nullptr) {
    return ctx->delete_stmt()->accept(this);
  }
  return absl::InvalidArgumentError("Unsupported statement type");
}

// Create Table constructs.
antlrcpp::Any BuildAstVisitor::visitCreate_table_stmt(
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
  std::unique_ptr<sqlast::CreateTable> table =
      std::make_unique<sqlast::CreateTable>(table_name);
  for (auto *column : ctx->column_def()) {
    CAST_OR_RETURN(sqlast::ColumnDefinition col, column->accept(this),
                   sqlast::ColumnDefinition);
    table->AddColumn(col.column_name(), col);
  }
  for (auto *constraint : ctx->table_constraint()) {
    CAST_OR_RETURN(const auto &[col_name _COMMA ast_constraint],
                   constraint->accept(this),
                   std::pair<std::string _COMMA sqlast::ColumnConstraint>);
    if (!table->HasColumn(col_name)) {
      return absl::InvalidArgumentError("Constraint over non-existing column");
    }
    table->MutableColumn(col_name).AddConstraint(ast_constraint);
  }

  // Upcast to an abstract statement.
  return static_cast<std::unique_ptr<sqlast::AbstractStatement>>(
      std::move(table));
}

antlrcpp::Any BuildAstVisitor::visitColumn_def(
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
  sqlast::ColumnDefinition column(column_name, column_type);
  for (auto *constraint : ctx->column_constraint()) {
    CAST_OR_RETURN(sqlast::ColumnConstraint constr, constraint->accept(this),
                   sqlast::ColumnConstraint);
    column.AddConstraint(constr);
  }
  return column;
}

antlrcpp::Any BuildAstVisitor::visitColumn_constraint(
    sqlparser::SQLiteParser::Column_constraintContext *ctx) {
  if (ctx->asc_desc() != nullptr || ctx->conflict_clause() != nullptr ||
      ctx->CHECK() != nullptr || ctx->DEFAULT() != nullptr ||
      ctx->COLLATE() != nullptr || ctx->AS() != nullptr ||
      ctx->CONSTRAINT() != nullptr || ctx->AUTOINCREMENT() != nullptr) {
    return absl::InvalidArgumentError("Invalid inline constraint");
  }

  if (ctx->PRIMARY() != nullptr) {
    return sqlast::ColumnConstraint(sqlast::ColumnConstraint::PRIMARY_KEY);
  }
  if (ctx->NOT() != nullptr) {
    return sqlast::ColumnConstraint(sqlast::ColumnConstraint::NOT_NULL);
  }
  if (ctx->UNIQUE() != nullptr) {
    return sqlast::ColumnConstraint(sqlast::ColumnConstraint::UNIQUE);
  }
  return ctx->foreign_key_clause()->accept(this);
}

antlrcpp::Any BuildAstVisitor::visitTable_constraint(
    sqlparser::SQLiteParser::Table_constraintContext *ctx) {
  if (ctx->CONSTRAINT() != nullptr || ctx->conflict_clause() != nullptr ||
      ctx->CHECK() != nullptr || ctx->column_name().size() > 1 ||
      ctx->indexed_column().size() > 1) {
    return absl::InvalidArgumentError("Invalid table constraint");
  }

  if (ctx->PRIMARY() != nullptr) {
    CAST_OR_RETURN(std::string col_name, ctx->indexed_column(0)->accept(this),
                   std::string);
    return std::make_pair(col_name, sqlast::ColumnConstraint(
                                        sqlast::ColumnConstraint::PRIMARY_KEY));
  }
  if (ctx->UNIQUE() != nullptr) {
    CAST_OR_RETURN(std::string col_name, ctx->indexed_column(0)->accept(this),
                   std::string);
    return std::make_pair(
        col_name, sqlast::ColumnConstraint(sqlast::ColumnConstraint::UNIQUE));
  }
  // Can only be a foreign key.
  CAST_OR_RETURN(std::string col_name, ctx->column_name(0)->accept(this),
                 std::string);
  CAST_OR_RETURN(sqlast::ColumnConstraint fk_constraint,
                 ctx->foreign_key_clause()->accept(this),
                 sqlast::ColumnConstraint);
  return std::make_pair(col_name, fk_constraint);
}

antlrcpp::Any BuildAstVisitor::visitForeign_key_clause(
    sqlparser::SQLiteParser::Foreign_key_clauseContext *ctx) {
  if (ctx->ON().size() > 0 || ctx->MATCH().size() > 0 ||
      ctx->DEFERRABLE() != nullptr || ctx->column_name().size() != 1) {
    return absl::InvalidArgumentError("Invalid foreign key constraint");
  }

  sqlast::ColumnConstraint constrn(sqlast::ColumnConstraint::Type::FOREIGN_KEY);
  CAST_OR_RETURN(constrn.foreign_table(), ctx->foreign_table()->accept(this),
                 std::string);
  CAST_OR_RETURN(constrn.foreign_column(), ctx->column_name(0)->accept(this),
                 std::string);
  return constrn;
}

// Insert constructs.
antlrcpp::Any BuildAstVisitor::visitInsert_stmt(
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
  std::unique_ptr<sqlast::Insert> insert =
      std::make_unique<sqlast::Insert>(table_name);
  for (auto column_name_ctx : ctx->column_name()) {
    CAST_OR_RETURN(std::string column_name, column_name_ctx->accept(this),
                   std::string);
    insert->AddColumn(column_name);
  }

  // Inserted values.
  CAST_OR_RETURN(std::vector<std::string> values,
                 ctx->expr_list(0)->accept(this), std::vector<std::string>);
  insert->SetValues(std::move(values));
  return static_cast<std::unique_ptr<sqlast::AbstractStatement>>(
      std::move(insert));
}
antlrcpp::Any BuildAstVisitor::visitExpr_list(
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

// Select constructs.
antlrcpp::Any BuildAstVisitor::visitSelect_stmt(
    sqlparser::SQLiteParser::Select_stmtContext *ctx) {
  if (ctx->common_table_stmt() != nullptr ||
      ctx->compound_operator().size() != 0 || ctx->select_core().size() != 1 ||
      ctx->order_by_stmt() != nullptr || ctx->limit_stmt() != nullptr) {
    return absl::InvalidArgumentError("Invalid select construct");
  }
  return ctx->select_core(0)->accept(this);
}
antlrcpp::Any BuildAstVisitor::visitSelect_core(
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
  std::unique_ptr<sqlast::Select> select =
      std::make_unique<sqlast::Select>(table_name);

  for (auto *result_column_ctx : ctx->result_column()) {
    CAST_OR_RETURN(std::string column, result_column_ctx->accept(this),
                   std::string);
    select->AddColumn(column);
  }

  if (ctx->WHERE() != nullptr) {
    MCAST_OR_RETURN(std::unique_ptr<sqlast::Expression> expr,
                    ctx->expr(0)->accept(this),
                    std::unique_ptr<sqlast::Expression>);
    if (expr->type() != sqlast::Expression::EQ &&
        expr->type() != sqlast::Expression::AND) {
      return absl::InvalidArgumentError("Where clause must be boolean");
    }

    std::unique_ptr<sqlast::BinaryExpression> bexpr(
        static_cast<sqlast::BinaryExpression *>(expr.release()));
    select->SetWhereClause(std::move(bexpr));
  }
  return static_cast<std::unique_ptr<sqlast::AbstractStatement>>(
      std::move(select));
}
antlrcpp::Any BuildAstVisitor::visitResult_column(
    sqlparser::SQLiteParser::Result_columnContext *ctx) {
  if (ctx->STAR() == nullptr) {
    return absl::InvalidArgumentError("Only support * in select");
  }
  return std::string("*");
}
antlrcpp::Any BuildAstVisitor::visitTable_or_subquery(
    sqlparser::SQLiteParser::Table_or_subqueryContext *ctx) {
  if (ctx->schema_name() != nullptr || ctx->AS() != nullptr ||
      ctx->table_alias() != nullptr || ctx->INDEXED() != nullptr ||
      ctx->table_name() == nullptr) {
    return absl::InvalidArgumentError("Invalid select domain");
  }
  return ctx->table_name()->accept(this);
}

// Delete statement.
antlrcpp::Any BuildAstVisitor::visitDelete_stmt(
    sqlparser::SQLiteParser::Delete_stmtContext *ctx) {
  if (ctx->with_clause() != nullptr) {
    return absl::InvalidArgumentError("Invalid delete statement");
  }

  // Find table name.
  CAST_OR_RETURN(std::string table_name,
                 ctx->qualified_table_name()->accept(this), std::string);
  std::unique_ptr<sqlast::Delete> delete_ =
      std::make_unique<sqlast::Delete>(table_name);
  // Where clause if exists.
  if (ctx->WHERE() != nullptr) {
    MCAST_OR_RETURN(std::unique_ptr<sqlast::Expression> expr,
                    ctx->expr()->accept(this),
                    std::unique_ptr<sqlast::Expression>);
    if (expr->type() != sqlast::Expression::EQ &&
        expr->type() != sqlast::Expression::AND) {
      return absl::InvalidArgumentError("Where clause must be boolean");
    }

    std::unique_ptr<sqlast::BinaryExpression> bexpr(
        static_cast<sqlast::BinaryExpression *>(expr.release()));
    delete_->SetWhereClause(std::move(bexpr));
  }
  return static_cast<std::unique_ptr<sqlast::AbstractStatement>>(
      std::move(delete_));
}
antlrcpp::Any BuildAstVisitor::visitQualified_table_name(
    sqlparser::SQLiteParser::Qualified_table_nameContext *ctx) {
  if (ctx->schema_name() != nullptr || ctx->AS() != nullptr ||
      ctx->INDEXED() != nullptr || ctx->INDEXED() != nullptr) {
    return absl::InvalidArgumentError("Invalid delete domain");
  }
  return ctx->table_name()->accept(this);
}

// Common constructs.
// Expressions.
antlrcpp::Any BuildAstVisitor::visitExpr(
    sqlparser::SQLiteParser::ExprContext *ctx) {
  if ((ctx->literal_value() == nullptr && ctx->ASSIGN() == nullptr &&
       ctx->column_name() == nullptr && ctx->AND() == nullptr) ||
      ctx->expr().size() > 2) {
    return absl::InvalidArgumentError("Unsupported expression");
  }

  if (ctx->literal_value() != nullptr) {
    CAST_OR_RETURN(std::string value, ctx->literal_value()->accept(this),
                   std::string);
    std::unique_ptr<sqlast::Expression> result =
        std::make_unique<sqlast::LiteralExpression>(value);
    return result;
  }
  if (ctx->column_name() != nullptr) {
    CAST_OR_RETURN(std::string column, ctx->column_name()->accept(this),
                   std::string);
    std::unique_ptr<sqlast::Expression> result =
        std::make_unique<sqlast::ColumnExpression>(column);
    return result;
  }

  std::unique_ptr<sqlast::BinaryExpression> result;
  if (ctx->ASSIGN() != nullptr) {
    result = std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::EQ);
  }
  if (ctx->AND() != nullptr) {
    result =
        std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::AND);
  }

  MCAST_OR_RETURN(std::unique_ptr<sqlast::Expression> expr0,
                  ctx->expr(0)->accept(this),
                  std::unique_ptr<sqlast::Expression>);
  MCAST_OR_RETURN(std::unique_ptr<sqlast::Expression> expr1,
                  ctx->expr(1)->accept(this),
                  std::unique_ptr<sqlast::Expression>);
  result->SetLeft(std::move(expr0));
  result->SetRight(std::move(expr1));
  return static_cast<std::unique_ptr<sqlast::Expression>>(std::move(result));
}
antlrcpp::Any BuildAstVisitor::visitLiteral_value(
    sqlparser::SQLiteParser::Literal_valueContext *ctx) {
  return ctx->getText();
}

// Names of entities (columns, tables, etc).
antlrcpp::Any BuildAstVisitor::visitIndexed_column(
    sqlparser::SQLiteParser::Indexed_columnContext *ctx) {
  if (ctx->COLLATE() != nullptr || ctx->expr() != nullptr ||
      ctx->collation_name() != nullptr || ctx->asc_desc() != nullptr ||
      ctx->column_name() == nullptr) {
    return absl::InvalidArgumentError("Invalid column name");
  }
  return ctx->column_name()->accept(this);
}
antlrcpp::Any BuildAstVisitor::visitName(
    sqlparser::SQLiteParser::NameContext *ctx) {
  return ctx->any_name()->accept(this);
}
antlrcpp::Any BuildAstVisitor::visitTable_name(
    sqlparser::SQLiteParser::Table_nameContext *ctx) {
  return ctx->any_name()->accept(this);
}
antlrcpp::Any BuildAstVisitor::visitForeign_table(
    sqlparser::SQLiteParser::Foreign_tableContext *ctx) {
  return ctx->any_name()->accept(this);
}
antlrcpp::Any BuildAstVisitor::visitColumn_name(
    sqlparser::SQLiteParser::Column_nameContext *ctx) {
  return ctx->any_name()->accept(this);
}
antlrcpp::Any BuildAstVisitor::visitType_name(
    sqlparser::SQLiteParser::Type_nameContext *ctx) {
  return ctx->getText();
}
antlrcpp::Any BuildAstVisitor::visitAny_name(
    sqlparser::SQLiteParser::Any_nameContext *ctx) {
  if (ctx->IDENTIFIER() == nullptr) {
    return absl::InvalidArgumentError("All names must be simple and unquoted");
  }
  return ctx->getText();
}

// Invalid Syntax!
antlrcpp::Any BuildAstVisitor::visitParse(
    sqlparser::SQLiteParser::ParseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitError(
    sqlparser::SQLiteParser::ErrorContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitSql_stmt_list(
    sqlparser::SQLiteParser::Sql_stmt_listContext *context) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitAlter_table_stmt(
    sqlparser::SQLiteParser::Alter_table_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitAnalyze_stmt(
    sqlparser::SQLiteParser::Analyze_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitAttach_stmt(
    sqlparser::SQLiteParser::Attach_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitBegin_stmt(
    sqlparser::SQLiteParser::Begin_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitCommit_stmt(
    sqlparser::SQLiteParser::Commit_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitRollback_stmt(
    sqlparser::SQLiteParser::Rollback_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitSavepoint_stmt(
    sqlparser::SQLiteParser::Savepoint_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitRelease_stmt(
    sqlparser::SQLiteParser::Release_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitCreate_index_stmt(
    sqlparser::SQLiteParser::Create_index_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitSigned_number(
    sqlparser::SQLiteParser::Signed_numberContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitConflict_clause(
    sqlparser::SQLiteParser::Conflict_clauseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitCreate_trigger_stmt(
    sqlparser::SQLiteParser::Create_trigger_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitCreate_view_stmt(
    sqlparser::SQLiteParser::Create_view_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitCreate_virtual_table_stmt(
    sqlparser::SQLiteParser::Create_virtual_table_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitWith_clause(
    sqlparser::SQLiteParser::With_clauseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitCte_table_name(
    sqlparser::SQLiteParser::Cte_table_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitRecursive_cte(
    sqlparser::SQLiteParser::Recursive_cteContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitCommon_table_expression(
    sqlparser::SQLiteParser::Common_table_expressionContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitDelete_stmt_limited(
    sqlparser::SQLiteParser::Delete_stmt_limitedContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitDetach_stmt(
    sqlparser::SQLiteParser::Detach_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitDrop_stmt(
    sqlparser::SQLiteParser::Drop_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitRaise_function(
    sqlparser::SQLiteParser::Raise_functionContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitUpsert_clause(
    sqlparser::SQLiteParser::Upsert_clauseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitPragma_stmt(
    sqlparser::SQLiteParser::Pragma_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitPragma_value(
    sqlparser::SQLiteParser::Pragma_valueContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitReindex_stmt(
    sqlparser::SQLiteParser::Reindex_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitJoin_clause(
    sqlparser::SQLiteParser::Join_clauseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitFactored_select_stmt(
    sqlparser::SQLiteParser::Factored_select_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitSimple_select_stmt(
    sqlparser::SQLiteParser::Simple_select_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitCompound_select_stmt(
    sqlparser::SQLiteParser::Compound_select_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitJoin_operator(
    sqlparser::SQLiteParser::Join_operatorContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitJoin_constraint(
    sqlparser::SQLiteParser::Join_constraintContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitCompound_operator(
    sqlparser::SQLiteParser::Compound_operatorContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitUpdate_stmt(
    sqlparser::SQLiteParser::Update_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitColumn_name_list(
    sqlparser::SQLiteParser::Column_name_listContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitUpdate_stmt_limited(
    sqlparser::SQLiteParser::Update_stmt_limitedContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitVacuum_stmt(
    sqlparser::SQLiteParser::Vacuum_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitFilter_clause(
    sqlparser::SQLiteParser::Filter_clauseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitWindow_defn(
    sqlparser::SQLiteParser::Window_defnContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitOver_clause(
    sqlparser::SQLiteParser::Over_clauseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitFrame_spec(
    sqlparser::SQLiteParser::Frame_specContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitFrame_clause(
    sqlparser::SQLiteParser::Frame_clauseContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitSimple_function_invocation(
    sqlparser::SQLiteParser::Simple_function_invocationContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitAggregate_function_invocation(
    sqlparser::SQLiteParser::Aggregate_function_invocationContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitWindow_function_invocation(
    sqlparser::SQLiteParser::Window_function_invocationContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitCommon_table_stmt(
    sqlparser::SQLiteParser::Common_table_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitOrder_by_stmt(
    sqlparser::SQLiteParser::Order_by_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitLimit_stmt(
    sqlparser::SQLiteParser::Limit_stmtContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitOrdering_term(
    sqlparser::SQLiteParser::Ordering_termContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitAsc_desc(
    sqlparser::SQLiteParser::Asc_descContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitFrame_left(
    sqlparser::SQLiteParser::Frame_leftContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitFrame_right(
    sqlparser::SQLiteParser::Frame_rightContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitFrame_single(
    sqlparser::SQLiteParser::Frame_singleContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitWindow_function(
    sqlparser::SQLiteParser::Window_functionContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitOffset(
    sqlparser::SQLiteParser::OffsetContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitDefault_value(
    sqlparser::SQLiteParser::Default_valueContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitPartition_by(
    sqlparser::SQLiteParser::Partition_byContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitOrder_by_expr(
    sqlparser::SQLiteParser::Order_by_exprContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitOrder_by_expr_asc_desc(
    sqlparser::SQLiteParser::Order_by_expr_asc_descContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitExpr_asc_desc(
    sqlparser::SQLiteParser::Expr_asc_descContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitInitial_select(
    sqlparser::SQLiteParser::Initial_selectContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitRecursive_select(
    sqlparser::SQLiteParser::Recursive_selectContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitUnary_operator(
    sqlparser::SQLiteParser::Unary_operatorContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitError_message(
    sqlparser::SQLiteParser::Error_messageContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitModule_argument(
    sqlparser::SQLiteParser::Module_argumentContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitColumn_alias(
    sqlparser::SQLiteParser::Column_aliasContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitKeyword(
    sqlparser::SQLiteParser::KeywordContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitSchema_name(
    sqlparser::SQLiteParser::Schema_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitFunction_name(
    sqlparser::SQLiteParser::Function_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitTable_or_index_name(
    sqlparser::SQLiteParser::Table_or_index_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitNew_table_name(
    sqlparser::SQLiteParser::New_table_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitCollation_name(
    sqlparser::SQLiteParser::Collation_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitIndex_name(
    sqlparser::SQLiteParser::Index_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitTrigger_name(
    sqlparser::SQLiteParser::Trigger_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitView_name(
    sqlparser::SQLiteParser::View_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitModule_name(
    sqlparser::SQLiteParser::Module_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitPragma_name(
    sqlparser::SQLiteParser::Pragma_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitSavepoint_name(
    sqlparser::SQLiteParser::Savepoint_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitTable_alias(
    sqlparser::SQLiteParser::Table_aliasContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitTransaction_name(
    sqlparser::SQLiteParser::Transaction_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitWindow_name(
    sqlparser::SQLiteParser::Window_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitAlias(
    sqlparser::SQLiteParser::AliasContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitFilename(
    sqlparser::SQLiteParser::FilenameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitBase_window_name(
    sqlparser::SQLiteParser::Base_window_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitSimple_func(
    sqlparser::SQLiteParser::Simple_funcContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitAggregate_func(
    sqlparser::SQLiteParser::Aggregate_funcContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}
antlrcpp::Any BuildAstVisitor::visitTable_function_name(
    sqlparser::SQLiteParser::Table_function_nameContext *ctx) {
  return absl::InvalidArgumentError("Unsupported Syntax");
}

}  // namespace visitors
}  // namespace sqlengine
}  // namespace shards
