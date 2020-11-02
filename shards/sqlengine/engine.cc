// Entry point for our sql rewriting engine.

#include "shards/sqlengine/engine.h"

#include "shards/sqlengine/parser.h"
// gives sqllparser::SQLiteParser::*
#include "SQLiteParser.h"
#include "shards/sqlengine/schema.h"

namespace shards {
namespace sqlengine {

void Rewrite(const std::string &sql, SharderState *state) {
  // Parse with ANTLR.
  parser::SQLParser parser;
  if (!parser.Parse(sql)) {
    return;
  }

  // Visit parsed statement!
  sqlparser::SQLiteParser::Sql_stmtContext *statement = parser.Statement();

  sqlparser::SQLiteParser::Create_table_stmtContext *create_stmt =
      statement->create_table_stmt();
  if (create_stmt != nullptr) {
    schema::Rewrite(create_stmt, state);
  }
}

}  // namespace sqlengine
}  // namespace shards
