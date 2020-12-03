// Delete statements sharding and rewriting.

#include "shards/sqlengine/delete.h"

#include <cstdio>
#include <utility>

#include "absl/strings/str_cat.h"
// NOLINTNEXTLINE
#include "antlr4-runtime.h"
#include "shards/sqlengine/util.h"
#include "shards/sqlengine/visitors/stringify.h"

namespace shards {
namespace sqlengine {
namespace delete_ {

namespace {

std::string FindExprVal(sqlparser::SQLiteParser::ExprContext *expr) {
  if (expr->column_name() != nullptr) {
    return "";
  }
  if (expr->literal_value() != nullptr) {
    return expr->literal_value()->getText();
  }
  return FindExprVal(expr->expr(0)) + FindExprVal(expr->expr(1));
}

}  // namespace

std::list<std::tuple<std::string, std::string, CallbackModifier>> Rewrite(
    sqlparser::SQLiteParser::Delete_stmtContext *stmt, SharderState *state) {
  const std::string &table_name =
      stmt->qualified_table_name()->table_name()->getText();
  // Turn the delete statement back to a string.
  visitors::Stringify stringify;
  std::string delete_str = stmt->accept(&stringify).as<std::string>();

  // Check if delete is deleting a user, in which case, also delete their shard!
  if (state->IsPII(table_name)) {
    if (stmt->expr() != nullptr) {
      std::string user_id = FindExprVal(stmt->expr());
      if (user_id.size() > 0) {
        std::string shard = sqlengine::NameShard(table_name, user_id);
        std::string path = absl::StrCat(state->dir_path(), shard, ".sqlite3");
        remove(path.c_str());
        state->RemoveUserFromShard(table_name, user_id);
      }
    }
  }

  std::list<std::tuple<std::string, std::string, CallbackModifier>> result;
  result.emplace_back(DEFAULT_SHARD_NAME, delete_str, identity_modifier);
  return result;
}

}  // namespace delete_
}  // namespace sqlengine
}  // namespace shards
