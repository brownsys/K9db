// A default ANTLR visitor that only visist explicitly supported
// syntax and throws an error if unsupproted syntax was used.

#include "shards/sqlengine/visitors/valid.h"

namespace shards {
namespace sqlengine {
namespace visitors {

// Override special functions.
antlrcpp::Any Valid::defaultResult() { return true; }
antlrcpp::Any Valid::visitTerminal(antlr4::tree::TerminalNode *_) {
  return true;
}
antlrcpp::Any Valid::visitErrorNode(antlr4::tree::ErrorNode *_) {
  return false;
}
antlrcpp::Any Valid::aggregateResult(antlrcpp::Any agg,
                                     const antlrcpp::Any &next) {
  return agg.as<bool>() && next.as<bool>();
}

// Valid Syntax.
antlrcpp::Any Valid::visitSql_stmt(
    sqlparser::SQLiteParser::Sql_stmtContext *ctx) {
  return visitChildren(ctx);
}

// Create Table constructs.
antlrcpp::Any Valid::visitCreate_table_stmt(
    sqlparser::SQLiteParser::Create_table_stmtContext *ctx) {
  if (ctx->TEMP() != nullptr || ctx->TEMPORARY() != nullptr ||
      ctx->EXISTS() != nullptr || ctx->schema_name() != nullptr ||
      ctx->WITHOUT() != nullptr || ctx->AS() != nullptr) {
    return false;
  }
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitColumn_def(
    sqlparser::SQLiteParser::Column_defContext *ctx) {
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitColumn_constraint(
    sqlparser::SQLiteParser::Column_constraintContext *ctx) {
  if (ctx->asc_desc() != nullptr || ctx->conflict_clause() != nullptr ||
      ctx->CHECK() != nullptr || ctx->DEFAULT() != nullptr ||
      ctx->COLLATE() != nullptr || ctx->AS() != nullptr) {
    return false;
  }
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitTable_constraint(
    sqlparser::SQLiteParser::Table_constraintContext *ctx) {
  if (ctx->conflict_clause() != nullptr || ctx->CHECK() != nullptr) {
    return false;
  }
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitForeign_key_clause(
    sqlparser::SQLiteParser::Foreign_key_clauseContext *ctx) {
  if (ctx->ON().size() > 0 || ctx->MATCH().size() > 0 ||
      ctx->DEFERRABLE() != nullptr) {
    return false;
  }
  return visitChildren(ctx);
}

// Insert constructs.
antlrcpp::Any Valid::visitInsert_stmt(
    sqlparser::SQLiteParser::Insert_stmtContext *ctx) {
  if (ctx->REPLACE() != nullptr || ctx->OR() != nullptr ||
      ctx->AS() != nullptr || ctx->select_stmt() != nullptr ||
      ctx->upsert_clause() != nullptr || ctx->DEFAULT() != nullptr) {
    return false;
  }
  if (ctx->expr_list().size() > 1) {
    return false;
  }
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitExpr_list(
    sqlparser::SQLiteParser::Expr_listContext *ctx) {
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitExpr(sqlparser::SQLiteParser::ExprContext *ctx) {
  if ((ctx->literal_value() == nullptr && ctx->ASSIGN() == nullptr &&
       ctx->column_name() == nullptr && ctx->AND() == nullptr) ||
      ctx->expr().size() > 2) {
    return false;
  }
  return visitChildren(ctx);
}

// Select constructs.
antlrcpp::Any Valid::visitSelect_stmt(
    sqlparser::SQLiteParser::Select_stmtContext *ctx) {
  if (ctx->common_table_stmt() != nullptr ||
      ctx->compound_operator().size() != 0 || ctx->select_core().size() != 1 ||
      ctx->order_by_stmt() != nullptr || ctx->limit_stmt() != nullptr) {
    return false;
  }
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitSelect_core(
    sqlparser::SQLiteParser::Select_coreContext *ctx) {
  if (ctx->DISTINCT() != nullptr || ctx->ALL() != nullptr ||
      ctx->result_column().size() != 1 ||
      ctx->table_or_subquery().size() != 1 || ctx->GROUP() != nullptr ||
      ctx->HAVING() != nullptr || ctx->WINDOW() != nullptr ||
      ctx->VALUES() != nullptr) {
    return false;
  }
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitResult_column(
    sqlparser::SQLiteParser::Result_columnContext *ctx) {
  return ctx->STAR() != nullptr;
}
antlrcpp::Any Valid::visitTable_or_subquery(
    sqlparser::SQLiteParser::Table_or_subqueryContext *ctx) {
  if (ctx->schema_name() != nullptr || ctx->AS() != nullptr ||
      ctx->INDEXED() != nullptr) {
    return false;
  }
  return visitChildren(ctx);
}

// Common constructs.
antlrcpp::Any Valid::visitIndexed_column(
    sqlparser::SQLiteParser::Indexed_columnContext *ctx) {
  if (ctx->COLLATE() != nullptr || ctx->expr() != nullptr ||
      ctx->collation_name() != nullptr || ctx->asc_desc() != nullptr ||
      ctx->column_name() == nullptr) {
    return false;
  }
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitAsc_desc(
    sqlparser::SQLiteParser::Asc_descContext *ctx) {
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitName(sqlparser::SQLiteParser::NameContext *ctx) {
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitSchema_name(
    sqlparser::SQLiteParser::Schema_nameContext *ctx) {
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitTable_name(
    sqlparser::SQLiteParser::Table_nameContext *ctx) {
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitForeign_table(
    sqlparser::SQLiteParser::Foreign_tableContext *ctx) {
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitColumn_name(
    sqlparser::SQLiteParser::Column_nameContext *ctx) {
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitType_name(
    sqlparser::SQLiteParser::Type_nameContext *ctx) {
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitAny_name(
    sqlparser::SQLiteParser::Any_nameContext *ctx) {
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitLiteral_value(
    sqlparser::SQLiteParser::Literal_valueContext *ctx) {
  return visitChildren(ctx);
}
antlrcpp::Any Valid::visitSigned_number(
    sqlparser::SQLiteParser::Signed_numberContext *ctx) {
  return visitChildren(ctx);
}

// Invalid Syntax!
antlrcpp::Any Valid::visitParse(sqlparser::SQLiteParser::ParseContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitError(sqlparser::SQLiteParser::ErrorContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitSql_stmt_list(
    sqlparser::SQLiteParser::Sql_stmt_listContext *context) {
  return false;
}
antlrcpp::Any Valid::visitAlter_table_stmt(
    sqlparser::SQLiteParser::Alter_table_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitAnalyze_stmt(
    sqlparser::SQLiteParser::Analyze_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitAttach_stmt(
    sqlparser::SQLiteParser::Attach_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitBegin_stmt(
    sqlparser::SQLiteParser::Begin_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitCommit_stmt(
    sqlparser::SQLiteParser::Commit_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitRollback_stmt(
    sqlparser::SQLiteParser::Rollback_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitSavepoint_stmt(
    sqlparser::SQLiteParser::Savepoint_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitRelease_stmt(
    sqlparser::SQLiteParser::Release_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitCreate_index_stmt(
    sqlparser::SQLiteParser::Create_index_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitConflict_clause(
    sqlparser::SQLiteParser::Conflict_clauseContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitCreate_trigger_stmt(
    sqlparser::SQLiteParser::Create_trigger_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitCreate_view_stmt(
    sqlparser::SQLiteParser::Create_view_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitCreate_virtual_table_stmt(
    sqlparser::SQLiteParser::Create_virtual_table_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitWith_clause(
    sqlparser::SQLiteParser::With_clauseContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitCte_table_name(
    sqlparser::SQLiteParser::Cte_table_nameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitRecursive_cte(
    sqlparser::SQLiteParser::Recursive_cteContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitCommon_table_expression(
    sqlparser::SQLiteParser::Common_table_expressionContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitDelete_stmt(
    sqlparser::SQLiteParser::Delete_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitDelete_stmt_limited(
    sqlparser::SQLiteParser::Delete_stmt_limitedContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitDetach_stmt(
    sqlparser::SQLiteParser::Detach_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitDrop_stmt(
    sqlparser::SQLiteParser::Drop_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitRaise_function(
    sqlparser::SQLiteParser::Raise_functionContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitUpsert_clause(
    sqlparser::SQLiteParser::Upsert_clauseContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitPragma_stmt(
    sqlparser::SQLiteParser::Pragma_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitPragma_value(
    sqlparser::SQLiteParser::Pragma_valueContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitReindex_stmt(
    sqlparser::SQLiteParser::Reindex_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitJoin_clause(
    sqlparser::SQLiteParser::Join_clauseContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitFactored_select_stmt(
    sqlparser::SQLiteParser::Factored_select_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitSimple_select_stmt(
    sqlparser::SQLiteParser::Simple_select_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitCompound_select_stmt(
    sqlparser::SQLiteParser::Compound_select_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitJoin_operator(
    sqlparser::SQLiteParser::Join_operatorContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitJoin_constraint(
    sqlparser::SQLiteParser::Join_constraintContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitCompound_operator(
    sqlparser::SQLiteParser::Compound_operatorContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitUpdate_stmt(
    sqlparser::SQLiteParser::Update_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitColumn_name_list(
    sqlparser::SQLiteParser::Column_name_listContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitUpdate_stmt_limited(
    sqlparser::SQLiteParser::Update_stmt_limitedContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitQualified_table_name(
    sqlparser::SQLiteParser::Qualified_table_nameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitVacuum_stmt(
    sqlparser::SQLiteParser::Vacuum_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitFilter_clause(
    sqlparser::SQLiteParser::Filter_clauseContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitWindow_defn(
    sqlparser::SQLiteParser::Window_defnContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitOver_clause(
    sqlparser::SQLiteParser::Over_clauseContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitFrame_spec(
    sqlparser::SQLiteParser::Frame_specContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitFrame_clause(
    sqlparser::SQLiteParser::Frame_clauseContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitSimple_function_invocation(
    sqlparser::SQLiteParser::Simple_function_invocationContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitAggregate_function_invocation(
    sqlparser::SQLiteParser::Aggregate_function_invocationContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitWindow_function_invocation(
    sqlparser::SQLiteParser::Window_function_invocationContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitCommon_table_stmt(
    sqlparser::SQLiteParser::Common_table_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitOrder_by_stmt(
    sqlparser::SQLiteParser::Order_by_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitLimit_stmt(
    sqlparser::SQLiteParser::Limit_stmtContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitOrdering_term(
    sqlparser::SQLiteParser::Ordering_termContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitFrame_left(
    sqlparser::SQLiteParser::Frame_leftContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitFrame_right(
    sqlparser::SQLiteParser::Frame_rightContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitFrame_single(
    sqlparser::SQLiteParser::Frame_singleContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitWindow_function(
    sqlparser::SQLiteParser::Window_functionContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitOffset(sqlparser::SQLiteParser::OffsetContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitDefault_value(
    sqlparser::SQLiteParser::Default_valueContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitPartition_by(
    sqlparser::SQLiteParser::Partition_byContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitOrder_by_expr(
    sqlparser::SQLiteParser::Order_by_exprContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitOrder_by_expr_asc_desc(
    sqlparser::SQLiteParser::Order_by_expr_asc_descContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitExpr_asc_desc(
    sqlparser::SQLiteParser::Expr_asc_descContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitInitial_select(
    sqlparser::SQLiteParser::Initial_selectContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitRecursive_select(
    sqlparser::SQLiteParser::Recursive_selectContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitUnary_operator(
    sqlparser::SQLiteParser::Unary_operatorContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitError_message(
    sqlparser::SQLiteParser::Error_messageContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitModule_argument(
    sqlparser::SQLiteParser::Module_argumentContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitColumn_alias(
    sqlparser::SQLiteParser::Column_aliasContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitKeyword(
    sqlparser::SQLiteParser::KeywordContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitFunction_name(
    sqlparser::SQLiteParser::Function_nameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitTable_or_index_name(
    sqlparser::SQLiteParser::Table_or_index_nameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitNew_table_name(
    sqlparser::SQLiteParser::New_table_nameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitCollation_name(
    sqlparser::SQLiteParser::Collation_nameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitIndex_name(
    sqlparser::SQLiteParser::Index_nameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitTrigger_name(
    sqlparser::SQLiteParser::Trigger_nameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitView_name(
    sqlparser::SQLiteParser::View_nameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitModule_name(
    sqlparser::SQLiteParser::Module_nameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitPragma_name(
    sqlparser::SQLiteParser::Pragma_nameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitSavepoint_name(
    sqlparser::SQLiteParser::Savepoint_nameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitTable_alias(
    sqlparser::SQLiteParser::Table_aliasContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitTransaction_name(
    sqlparser::SQLiteParser::Transaction_nameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitWindow_name(
    sqlparser::SQLiteParser::Window_nameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitAlias(sqlparser::SQLiteParser::AliasContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitFilename(
    sqlparser::SQLiteParser::FilenameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitBase_window_name(
    sqlparser::SQLiteParser::Base_window_nameContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitSimple_func(
    sqlparser::SQLiteParser::Simple_funcContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitAggregate_func(
    sqlparser::SQLiteParser::Aggregate_funcContext *ctx) {
  return false;
}
antlrcpp::Any Valid::visitTable_function_name(
    sqlparser::SQLiteParser::Table_function_nameContext *ctx) {
  return false;
}

}  // namespace visitors
}  // namespace sqlengine
}  // namespace shards
