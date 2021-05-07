// Turn an ANTLR parse tree into a valid SQL string.
#ifndef PELTON_SQLAST_TRANSFORMER_H_
#define PELTON_SQLAST_TRANSFORMER_H_

#include <memory>

#include "SQLiteParserBaseVisitor.h"
#include "absl/status/statusor.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace sqlast {

class AstTransformer : public sqlparser::SQLiteParserBaseVisitor {
 public:
  AstTransformer() : in_update_(false) {}

  // Entry point for cst to ast transformation / building.
  absl::StatusOr<std::unique_ptr<sqlast::AbstractStatement>> TransformStatement(
      sqlparser::SQLiteParser::Sql_stmtContext *context);

  // Visitors for all possible parser rules from the sqlite3 grammar, including
  // supported (valid) and unsupported syntax.
  antlrcpp::Any visitParse(
      sqlparser::SQLiteParser::ParseContext *context) override;
  antlrcpp::Any visitError(
      sqlparser::SQLiteParser::ErrorContext *context) override;
  antlrcpp::Any visitSql_stmt_list(
      sqlparser::SQLiteParser::Sql_stmt_listContext *context) override;
  antlrcpp::Any visitSql_stmt(
      sqlparser::SQLiteParser::Sql_stmtContext *context) override;
  antlrcpp::Any visitAlter_table_stmt(
      sqlparser::SQLiteParser::Alter_table_stmtContext *context) override;
  antlrcpp::Any visitAnalyze_stmt(
      sqlparser::SQLiteParser::Analyze_stmtContext *context) override;
  antlrcpp::Any visitAttach_stmt(
      sqlparser::SQLiteParser::Attach_stmtContext *context) override;
  antlrcpp::Any visitBegin_stmt(
      sqlparser::SQLiteParser::Begin_stmtContext *context) override;
  antlrcpp::Any visitCommit_stmt(
      sqlparser::SQLiteParser::Commit_stmtContext *context) override;
  antlrcpp::Any visitRollback_stmt(
      sqlparser::SQLiteParser::Rollback_stmtContext *context) override;
  antlrcpp::Any visitSavepoint_stmt(
      sqlparser::SQLiteParser::Savepoint_stmtContext *context) override;
  antlrcpp::Any visitRelease_stmt(
      sqlparser::SQLiteParser::Release_stmtContext *context) override;
  antlrcpp::Any visitCreate_index_stmt(
      sqlparser::SQLiteParser::Create_index_stmtContext *context) override;
  antlrcpp::Any visitIndexed_column(
      sqlparser::SQLiteParser::Indexed_columnContext *context) override;
  antlrcpp::Any visitCreate_table_stmt(
      sqlparser::SQLiteParser::Create_table_stmtContext *context) override;
  antlrcpp::Any visitColumn_def(
      sqlparser::SQLiteParser::Column_defContext *context) override;
  antlrcpp::Any visitType_name(
      sqlparser::SQLiteParser::Type_nameContext *context) override;
  antlrcpp::Any visitColumn_constraint(
      sqlparser::SQLiteParser::Column_constraintContext *context) override;
  antlrcpp::Any visitSigned_number(
      sqlparser::SQLiteParser::Signed_numberContext *context) override;
  antlrcpp::Any visitTable_constraint(
      sqlparser::SQLiteParser::Table_constraintContext *context) override;
  antlrcpp::Any visitForeign_key_clause(
      sqlparser::SQLiteParser::Foreign_key_clauseContext *context) override;
  antlrcpp::Any visitConflict_clause(
      sqlparser::SQLiteParser::Conflict_clauseContext *context) override;
  antlrcpp::Any visitCreate_trigger_stmt(
      sqlparser::SQLiteParser::Create_trigger_stmtContext *context) override;
  antlrcpp::Any visitCreate_view_stmt(
      sqlparser::SQLiteParser::Create_view_stmtContext *context) override;
  antlrcpp::Any visitCreate_virtual_table_stmt(
      sqlparser::SQLiteParser::Create_virtual_table_stmtContext *context)
      override;
  antlrcpp::Any visitWith_clause(
      sqlparser::SQLiteParser::With_clauseContext *context) override;
  antlrcpp::Any visitCte_table_name(
      sqlparser::SQLiteParser::Cte_table_nameContext *context) override;
  antlrcpp::Any visitRecursive_cte(
      sqlparser::SQLiteParser::Recursive_cteContext *context) override;
  antlrcpp::Any visitCommon_table_expression(
      sqlparser::SQLiteParser::Common_table_expressionContext *context)
      override;
  antlrcpp::Any visitDelete_stmt(
      sqlparser::SQLiteParser::Delete_stmtContext *context) override;
  antlrcpp::Any visitDelete_stmt_limited(
      sqlparser::SQLiteParser::Delete_stmt_limitedContext *context) override;
  antlrcpp::Any visitDetach_stmt(
      sqlparser::SQLiteParser::Detach_stmtContext *context) override;
  antlrcpp::Any visitDrop_stmt(
      sqlparser::SQLiteParser::Drop_stmtContext *context) override;
  antlrcpp::Any visitExpr_list(
      sqlparser::SQLiteParser::Expr_listContext *ctx) override;
  antlrcpp::Any visitExpr(
      sqlparser::SQLiteParser::ExprContext *context) override;
  antlrcpp::Any visitRaise_function(
      sqlparser::SQLiteParser::Raise_functionContext *context) override;
  antlrcpp::Any visitLiteral_value(
      sqlparser::SQLiteParser::Literal_valueContext *context) override;
  antlrcpp::Any visitInsert_stmt(
      sqlparser::SQLiteParser::Insert_stmtContext *context) override;
  antlrcpp::Any visitUpsert_clause(
      sqlparser::SQLiteParser::Upsert_clauseContext *context) override;
  antlrcpp::Any visitPragma_stmt(
      sqlparser::SQLiteParser::Pragma_stmtContext *context) override;
  antlrcpp::Any visitPragma_value(
      sqlparser::SQLiteParser::Pragma_valueContext *context) override;
  antlrcpp::Any visitReindex_stmt(
      sqlparser::SQLiteParser::Reindex_stmtContext *context) override;
  antlrcpp::Any visitSelect_stmt(
      sqlparser::SQLiteParser::Select_stmtContext *context) override;
  antlrcpp::Any visitJoin_clause(
      sqlparser::SQLiteParser::Join_clauseContext *context) override;
  antlrcpp::Any visitSelect_core(
      sqlparser::SQLiteParser::Select_coreContext *context) override;
  antlrcpp::Any visitFactored_select_stmt(
      sqlparser::SQLiteParser::Factored_select_stmtContext *context) override;
  antlrcpp::Any visitSimple_select_stmt(
      sqlparser::SQLiteParser::Simple_select_stmtContext *context) override;
  antlrcpp::Any visitCompound_select_stmt(
      sqlparser::SQLiteParser::Compound_select_stmtContext *context) override;
  antlrcpp::Any visitTable_or_subquery(
      sqlparser::SQLiteParser::Table_or_subqueryContext *context) override;
  antlrcpp::Any visitResult_column(
      sqlparser::SQLiteParser::Result_columnContext *context) override;
  antlrcpp::Any visitJoin_operator(
      sqlparser::SQLiteParser::Join_operatorContext *context) override;
  antlrcpp::Any visitJoin_constraint(
      sqlparser::SQLiteParser::Join_constraintContext *context) override;
  antlrcpp::Any visitCompound_operator(
      sqlparser::SQLiteParser::Compound_operatorContext *context) override;
  antlrcpp::Any visitUpdate_stmt(
      sqlparser::SQLiteParser::Update_stmtContext *context) override;
  antlrcpp::Any visitColumn_name_list(
      sqlparser::SQLiteParser::Column_name_listContext *context) override;
  antlrcpp::Any visitUpdate_stmt_limited(
      sqlparser::SQLiteParser::Update_stmt_limitedContext *context) override;
  antlrcpp::Any visitQualified_table_name(
      sqlparser::SQLiteParser::Qualified_table_nameContext *context) override;
  antlrcpp::Any visitVacuum_stmt(
      sqlparser::SQLiteParser::Vacuum_stmtContext *context) override;
  antlrcpp::Any visitFilter_clause(
      sqlparser::SQLiteParser::Filter_clauseContext *context) override;
  antlrcpp::Any visitWindow_defn(
      sqlparser::SQLiteParser::Window_defnContext *context) override;
  antlrcpp::Any visitOver_clause(
      sqlparser::SQLiteParser::Over_clauseContext *context) override;
  antlrcpp::Any visitFrame_spec(
      sqlparser::SQLiteParser::Frame_specContext *context) override;
  antlrcpp::Any visitFrame_clause(
      sqlparser::SQLiteParser::Frame_clauseContext *context) override;
  antlrcpp::Any visitSimple_function_invocation(
      sqlparser::SQLiteParser::Simple_function_invocationContext *context)
      override;
  antlrcpp::Any visitAggregate_function_invocation(
      sqlparser::SQLiteParser::Aggregate_function_invocationContext *context)
      override;
  antlrcpp::Any visitWindow_function_invocation(
      sqlparser::SQLiteParser::Window_function_invocationContext *context)
      override;
  antlrcpp::Any visitCommon_table_stmt(
      sqlparser::SQLiteParser::Common_table_stmtContext *context) override;
  antlrcpp::Any visitOrder_by_stmt(
      sqlparser::SQLiteParser::Order_by_stmtContext *context) override;
  antlrcpp::Any visitLimit_stmt(
      sqlparser::SQLiteParser::Limit_stmtContext *context) override;
  antlrcpp::Any visitOrdering_term(
      sqlparser::SQLiteParser::Ordering_termContext *context) override;
  antlrcpp::Any visitAsc_desc(
      sqlparser::SQLiteParser::Asc_descContext *context) override;
  antlrcpp::Any visitFrame_left(
      sqlparser::SQLiteParser::Frame_leftContext *context) override;
  antlrcpp::Any visitFrame_right(
      sqlparser::SQLiteParser::Frame_rightContext *context) override;
  antlrcpp::Any visitFrame_single(
      sqlparser::SQLiteParser::Frame_singleContext *context) override;
  antlrcpp::Any visitWindow_function(
      sqlparser::SQLiteParser::Window_functionContext *context) override;
  antlrcpp::Any visitOffset(
      sqlparser::SQLiteParser::OffsetContext *context) override;
  antlrcpp::Any visitDefault_value(
      sqlparser::SQLiteParser::Default_valueContext *context) override;
  antlrcpp::Any visitPartition_by(
      sqlparser::SQLiteParser::Partition_byContext *context) override;
  antlrcpp::Any visitOrder_by_expr(
      sqlparser::SQLiteParser::Order_by_exprContext *context) override;
  antlrcpp::Any visitOrder_by_expr_asc_desc(
      sqlparser::SQLiteParser::Order_by_expr_asc_descContext *context) override;
  antlrcpp::Any visitExpr_asc_desc(
      sqlparser::SQLiteParser::Expr_asc_descContext *context) override;
  antlrcpp::Any visitInitial_select(
      sqlparser::SQLiteParser::Initial_selectContext *context) override;
  antlrcpp::Any visitRecursive_select(
      sqlparser::SQLiteParser::Recursive_selectContext *context) override;
  antlrcpp::Any visitUnary_operator(
      sqlparser::SQLiteParser::Unary_operatorContext *context) override;
  antlrcpp::Any visitError_message(
      sqlparser::SQLiteParser::Error_messageContext *context) override;
  antlrcpp::Any visitModule_argument(
      sqlparser::SQLiteParser::Module_argumentContext *context) override;
  antlrcpp::Any visitColumn_alias(
      sqlparser::SQLiteParser::Column_aliasContext *context) override;
  antlrcpp::Any visitKeyword(
      sqlparser::SQLiteParser::KeywordContext *context) override;
  antlrcpp::Any visitName(
      sqlparser::SQLiteParser::NameContext *context) override;
  antlrcpp::Any visitFunction_name(
      sqlparser::SQLiteParser::Function_nameContext *context) override;
  antlrcpp::Any visitSchema_name(
      sqlparser::SQLiteParser::Schema_nameContext *context) override;
  antlrcpp::Any visitTable_name(
      sqlparser::SQLiteParser::Table_nameContext *context) override;
  antlrcpp::Any visitTable_or_index_name(
      sqlparser::SQLiteParser::Table_or_index_nameContext *context) override;
  antlrcpp::Any visitNew_table_name(
      sqlparser::SQLiteParser::New_table_nameContext *context) override;
  antlrcpp::Any visitColumn_name(
      sqlparser::SQLiteParser::Column_nameContext *context) override;
  antlrcpp::Any visitCollation_name(
      sqlparser::SQLiteParser::Collation_nameContext *context) override;
  antlrcpp::Any visitForeign_table(
      sqlparser::SQLiteParser::Foreign_tableContext *context) override;
  antlrcpp::Any visitIndex_name(
      sqlparser::SQLiteParser::Index_nameContext *context) override;
  antlrcpp::Any visitTrigger_name(
      sqlparser::SQLiteParser::Trigger_nameContext *context) override;
  antlrcpp::Any visitView_name(
      sqlparser::SQLiteParser::View_nameContext *context) override;
  antlrcpp::Any visitModule_name(
      sqlparser::SQLiteParser::Module_nameContext *context) override;
  antlrcpp::Any visitPragma_name(
      sqlparser::SQLiteParser::Pragma_nameContext *context) override;
  antlrcpp::Any visitSavepoint_name(
      sqlparser::SQLiteParser::Savepoint_nameContext *context) override;
  antlrcpp::Any visitTable_alias(
      sqlparser::SQLiteParser::Table_aliasContext *context) override;
  antlrcpp::Any visitTransaction_name(
      sqlparser::SQLiteParser::Transaction_nameContext *context) override;
  antlrcpp::Any visitWindow_name(
      sqlparser::SQLiteParser::Window_nameContext *context) override;
  antlrcpp::Any visitAlias(
      sqlparser::SQLiteParser::AliasContext *context) override;
  antlrcpp::Any visitFilename(
      sqlparser::SQLiteParser::FilenameContext *context) override;
  antlrcpp::Any visitBase_window_name(
      sqlparser::SQLiteParser::Base_window_nameContext *context) override;
  antlrcpp::Any visitSimple_func(
      sqlparser::SQLiteParser::Simple_funcContext *context) override;
  antlrcpp::Any visitAggregate_func(
      sqlparser::SQLiteParser::Aggregate_funcContext *context) override;
  antlrcpp::Any visitTable_function_name(
      sqlparser::SQLiteParser::Table_function_nameContext *context) override;
  antlrcpp::Any visitAny_name(
      sqlparser::SQLiteParser::Any_nameContext *context) override;

 private:
  bool in_update_;
};
}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_TRANSFORMER_H_
