// A default ANTLR visitor that only visist explicitly supported
// syntax and throws an error if unsupproted syntax was used.

#ifndef SHARDS_SQLENGINE_VISITORS_DEFAULT_H_
#define SHARDS_SQLENGINE_VISITORS_DEFAULT_H_

#include <string>

#include "SQLiteParserBaseVisitor.h"

namespace shards {
namespace sqlengine {
namespace visitors {

class Default : public sqlparser::SQLiteParserBaseVisitor {
 public:
  antlrcpp::Any visitErrorNode(antlr4::tree::ErrorNode *_) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitParse(
      sqlparser::SQLiteParser::ParseContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitError(
      sqlparser::SQLiteParser::ErrorContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitSql_stmt_list(
      sqlparser::SQLiteParser::Sql_stmt_listContext *context) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitAlter_table_stmt(
      sqlparser::SQLiteParser::Alter_table_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitAnalyze_stmt(
      sqlparser::SQLiteParser::Analyze_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitAttach_stmt(
      sqlparser::SQLiteParser::Attach_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitBegin_stmt(
      sqlparser::SQLiteParser::Begin_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitCommit_stmt(
      sqlparser::SQLiteParser::Commit_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitRollback_stmt(
      sqlparser::SQLiteParser::Rollback_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitSavepoint_stmt(
      sqlparser::SQLiteParser::Savepoint_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitRelease_stmt(
      sqlparser::SQLiteParser::Release_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitCreate_index_stmt(
      sqlparser::SQLiteParser::Create_index_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitConflict_clause(
      sqlparser::SQLiteParser::Conflict_clauseContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitCreate_trigger_stmt(
      sqlparser::SQLiteParser::Create_trigger_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitCreate_view_stmt(
      sqlparser::SQLiteParser::Create_view_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitCreate_virtual_table_stmt(
      sqlparser::SQLiteParser::Create_virtual_table_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitWith_clause(
      sqlparser::SQLiteParser::With_clauseContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitCte_table_name(
      sqlparser::SQLiteParser::Cte_table_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitRecursive_cte(
      sqlparser::SQLiteParser::Recursive_cteContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitCommon_table_expression(
      sqlparser::SQLiteParser::Common_table_expressionContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitDelete_stmt(
      sqlparser::SQLiteParser::Delete_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitDelete_stmt_limited(
      sqlparser::SQLiteParser::Delete_stmt_limitedContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitDetach_stmt(
      sqlparser::SQLiteParser::Detach_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitDrop_stmt(
      sqlparser::SQLiteParser::Drop_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitExpr_list(
      sqlparser::SQLiteParser::Expr_listContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitExpr(sqlparser::SQLiteParser::ExprContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitRaise_function(
      sqlparser::SQLiteParser::Raise_functionContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitInsert_stmt(
      sqlparser::SQLiteParser::Insert_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitUpsert_clause(
      sqlparser::SQLiteParser::Upsert_clauseContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitPragma_stmt(
      sqlparser::SQLiteParser::Pragma_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitPragma_value(
      sqlparser::SQLiteParser::Pragma_valueContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitReindex_stmt(
      sqlparser::SQLiteParser::Reindex_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitJoin_clause(
      sqlparser::SQLiteParser::Join_clauseContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitFactored_select_stmt(
      sqlparser::SQLiteParser::Factored_select_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitSimple_select_stmt(
      sqlparser::SQLiteParser::Simple_select_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitCompound_select_stmt(
      sqlparser::SQLiteParser::Compound_select_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitResult_column(
      sqlparser::SQLiteParser::Result_columnContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitJoin_operator(
      sqlparser::SQLiteParser::Join_operatorContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitJoin_constraint(
      sqlparser::SQLiteParser::Join_constraintContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitCompound_operator(
      sqlparser::SQLiteParser::Compound_operatorContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitUpdate_stmt(
      sqlparser::SQLiteParser::Update_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitColumn_name_list(
      sqlparser::SQLiteParser::Column_name_listContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitUpdate_stmt_limited(
      sqlparser::SQLiteParser::Update_stmt_limitedContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitQualified_table_name(
      sqlparser::SQLiteParser::Qualified_table_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitVacuum_stmt(
      sqlparser::SQLiteParser::Vacuum_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitFilter_clause(
      sqlparser::SQLiteParser::Filter_clauseContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitWindow_defn(
      sqlparser::SQLiteParser::Window_defnContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitOver_clause(
      sqlparser::SQLiteParser::Over_clauseContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitFrame_spec(
      sqlparser::SQLiteParser::Frame_specContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitFrame_clause(
      sqlparser::SQLiteParser::Frame_clauseContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitSimple_function_invocation(
      sqlparser::SQLiteParser::Simple_function_invocationContext *ctx)
      override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitAggregate_function_invocation(
      sqlparser::SQLiteParser::Aggregate_function_invocationContext *ctx)
      override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitWindow_function_invocation(
      sqlparser::SQLiteParser::Window_function_invocationContext *ctx)
      override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitCommon_table_stmt(
      sqlparser::SQLiteParser::Common_table_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitOrder_by_stmt(
      sqlparser::SQLiteParser::Order_by_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitLimit_stmt(
      sqlparser::SQLiteParser::Limit_stmtContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitOrdering_term(
      sqlparser::SQLiteParser::Ordering_termContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitFrame_left(
      sqlparser::SQLiteParser::Frame_leftContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitFrame_right(
      sqlparser::SQLiteParser::Frame_rightContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitFrame_single(
      sqlparser::SQLiteParser::Frame_singleContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitWindow_function(
      sqlparser::SQLiteParser::Window_functionContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitOffset(
      sqlparser::SQLiteParser::OffsetContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitDefault_value(
      sqlparser::SQLiteParser::Default_valueContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitPartition_by(
      sqlparser::SQLiteParser::Partition_byContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitOrder_by_expr(
      sqlparser::SQLiteParser::Order_by_exprContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitOrder_by_expr_asc_desc(
      sqlparser::SQLiteParser::Order_by_expr_asc_descContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitExpr_asc_desc(
      sqlparser::SQLiteParser::Expr_asc_descContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitInitial_select(
      sqlparser::SQLiteParser::Initial_selectContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitRecursive_select(
      sqlparser::SQLiteParser::Recursive_selectContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitUnary_operator(
      sqlparser::SQLiteParser::Unary_operatorContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitError_message(
      sqlparser::SQLiteParser::Error_messageContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitModule_argument(
      sqlparser::SQLiteParser::Module_argumentContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitColumn_alias(
      sqlparser::SQLiteParser::Column_aliasContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitKeyword(
      sqlparser::SQLiteParser::KeywordContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitFunction_name(
      sqlparser::SQLiteParser::Function_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitTable_or_index_name(
      sqlparser::SQLiteParser::Table_or_index_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitNew_table_name(
      sqlparser::SQLiteParser::New_table_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitCollation_name(
      sqlparser::SQLiteParser::Collation_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitIndex_name(
      sqlparser::SQLiteParser::Index_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitTrigger_name(
      sqlparser::SQLiteParser::Trigger_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitView_name(
      sqlparser::SQLiteParser::View_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitModule_name(
      sqlparser::SQLiteParser::Module_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitPragma_name(
      sqlparser::SQLiteParser::Pragma_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitSavepoint_name(
      sqlparser::SQLiteParser::Savepoint_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitTable_alias(
      sqlparser::SQLiteParser::Table_aliasContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitTransaction_name(
      sqlparser::SQLiteParser::Transaction_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitWindow_name(
      sqlparser::SQLiteParser::Window_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitAlias(
      sqlparser::SQLiteParser::AliasContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitFilename(
      sqlparser::SQLiteParser::FilenameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitBase_window_name(
      sqlparser::SQLiteParser::Base_window_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitSimple_func(
      sqlparser::SQLiteParser::Simple_funcContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitAggregate_func(
      sqlparser::SQLiteParser::Aggregate_funcContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }

  antlrcpp::Any visitTable_function_name(
      sqlparser::SQLiteParser::Table_function_nameContext *ctx) override {
    throw "Unsupported SQL Syntax!";
  }
};

}  // namespace visitors
}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_VISITORS_DEFAULT_H_
