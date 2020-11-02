// Turn an ANTLR parse tree into a valid SQL string.

#ifndef SHARDS_SQLENGINE_VISITORS_STRINGIFY_H_
#define SHARDS_SQLENGINE_VISITORS_STRINGIFY_H_

#include <string>

#include "shards/sqlengine/visitors/default.h"

namespace shards {
namespace sqlengine {
namespace visitors {

class Stringify : public Default {
 public:
  // Default value!
  antlrcpp::Any defaultResult() override;

  // Terminal (literal) node!
  antlrcpp::Any visitTerminal(antlr4::tree::TerminalNode *node) override;

  // Aggregation
  antlrcpp::Any aggregateResult(antlrcpp::Any agg,
                                const antlrcpp::Any &next) override;

  // Visit an sql statement!
  antlrcpp::Any visitSql_stmt(
      sqlparser::SQLiteParser::Sql_stmtContext *ctx) override;

  // Create table.
  antlrcpp::Any visitCreate_table_stmt(
      sqlparser::SQLiteParser::Create_table_stmtContext *ctx) override;
  antlrcpp::Any visitColumn_def(
      sqlparser::SQLiteParser::Column_defContext *ctx) override;
  antlrcpp::Any visitColumn_constraint(
      sqlparser::SQLiteParser::Column_constraintContext *ctx) override;
  antlrcpp::Any visitTable_constraint(
      sqlparser::SQLiteParser::Table_constraintContext *ctx) override;
  // covers: REFERENCES ...
  antlrcpp::Any visitForeign_key_clause(
      sqlparser::SQLiteParser::Foreign_key_clauseContext *ctx) override;

  // Building blocks.
  antlrcpp::Any visitIndexed_column(
      sqlparser::SQLiteParser::Indexed_columnContext *ctx) override;
  antlrcpp::Any visitAsc_desc(
      sqlparser::SQLiteParser::Asc_descContext *ctx) override;

  // Names.
  antlrcpp::Any visitName(sqlparser::SQLiteParser::NameContext *ctx) override;
  antlrcpp::Any visitSchema_name(
      sqlparser::SQLiteParser::Schema_nameContext *ctx) override;
  antlrcpp::Any visitTable_name(
      sqlparser::SQLiteParser::Table_nameContext *ctx) override;
  antlrcpp::Any visitForeign_table(
      sqlparser::SQLiteParser::Foreign_tableContext *ctx) override;
  antlrcpp::Any visitColumn_name(
      sqlparser::SQLiteParser::Column_nameContext *ctx) override;
  antlrcpp::Any visitType_name(
      sqlparser::SQLiteParser::Type_nameContext *ctx) override;
  antlrcpp::Any visitAny_name(
      sqlparser::SQLiteParser::Any_nameContext *ctx) override;

  // Values.
  antlrcpp::Any visitLiteral_value(
      sqlparser::SQLiteParser::Literal_valueContext *ctx) override;
  antlrcpp::Any visitSigned_number(
      sqlparser::SQLiteParser::Signed_numberContext *ctx) override;
};

}  // namespace visitors
}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_VISITORS_STRINGIFY_H_
