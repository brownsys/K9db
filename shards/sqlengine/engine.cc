// Entry point for our sql rewriting engine.

#include "shards/sqlengine/engine.h"

#include "shards/sqlengine/parser.h"
// gives sqllparser::SQLiteParser::*
#include "SQLiteParser.h"
#include "shards/sqlengine/create.h"
#include "shards/sqlengine/insert.h"

namespace shards {
namespace sqlengine {

std::list<std::pair<ShardSuffix, SQLStatement>> Rewrite(const std::string &sql,
                                                        SharderState *state) {
  // Parse with ANTLR.
  parser::SQLParser parser;
  if (!parser.Parse(sql)) {
    return {};
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

  return {};
}

}  // namespace sqlengine
}  // namespace shards
