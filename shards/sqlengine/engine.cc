// Entry point for our sql rewriting engine.

#include "shards/sqlengine/engine.h"

#include "shards/sqlengine/parser.h"
// gives sqllparser::SQLiteParser::*
#include "SQLiteParser.h"
#include "shards/sqlengine/create.h"
#include "shards/sqlengine/delete.h"
#include "shards/sqlengine/insert.h"
#include "shards/sqlengine/select.h"

namespace shards {
namespace sqlengine {

std::list<std::tuple<ShardSuffix, SQLStatement, CallbackModifier>> Rewrite(
    const std::string &sql, SharderState *state) {
  // Parse with ANTLR.
  parser::SQLParser parser;
  if (!parser.Parse(sql)) {
    throw "SQL statement has unsupported construct or syntax!";
  }

  // Visit parsed statement!
  sqlparser::SQLiteParser::Sql_stmtContext *statement = parser.Statement();

  // Case 1: CREATE TABLE statement.
  sqlparser::SQLiteParser::Create_table_stmtContext *create_stmt =
      statement->create_table_stmt();
  if (create_stmt != nullptr) {
    return create::Rewrite(create_stmt, state);
  }

  // Case 2: Insert statement.
  sqlparser::SQLiteParser::Insert_stmtContext *insert_stmt =
      statement->insert_stmt();
  if (insert_stmt != nullptr) {
    return insert::Rewrite(insert_stmt, state);
  }

  // Case 4: Select statement.
  sqlparser::SQLiteParser::Select_stmtContext *select_stmt =
      statement->select_stmt();
  if (select_stmt != nullptr) {
    return select::Rewrite(select_stmt, state);
  }

  // Case 5: Delete statement.
  sqlparser::SQLiteParser::Delete_stmtContext *delete_stmt =
      statement->delete_stmt();
  if (delete_stmt != nullptr) {
    return delete_::Rewrite(delete_stmt, state);
  }

  throw "Unsupported SQL statement!";
}

}  // namespace sqlengine
}  // namespace shards
