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

// Find and return whatever value (if any) is assigned to the given column
// inside this expression.
std::pair<std::string, bool> FindColumnValue(
    sqlparser::SQLiteParser::ExprContext *expr,
    const std::string &pk_col_name) {
  if (expr->column_name() != nullptr) {
    return std::make_pair("", expr->column_name()->getText() == pk_col_name);
  }
  if (expr->literal_value() != nullptr) {
    return std::make_pair(expr->literal_value()->getText(), false);
  }
  if (expr->ASSIGN() != nullptr) {
    const auto &[val1, found1] = FindColumnValue(expr->expr(0), pk_col_name);
    const auto &[val2, found2] = FindColumnValue(expr->expr(1), pk_col_name);
    if (found1) {
      return std::make_pair(val2, true);
    }
    if (found2) {
      return std::make_pair(val1, true);
    }
    return std::make_pair("", false);
  }
  throw "DELETE PII statement has unsupported complex WHERE statement!";
}

}  // namespace

std::list<std::tuple<std::string, std::string, CallbackModifier>> Rewrite(
    sqlparser::SQLiteParser::Delete_stmtContext *stmt, SharderState *state) {
  std::list<std::tuple<std::string, std::string, CallbackModifier>> result;

  const std::string &table_name =
      stmt->qualified_table_name()->table_name()->getText();
  bool is_sharded = state->IsSharded(table_name);
  bool is_pii = state->IsPII(table_name);
  visitors::Stringify stringify;

  // Case 1: Table has PII.
  if (is_pii) {
    // We are deleting a user, we should also delete their shard!
    std::string user_id;
    bool found_user_id = false;
    if (stmt->expr() != nullptr) {
      const auto &[user_id, flag] =
          FindColumnValue(stmt->expr(), state->PkOfPII(table_name));
      if (flag) {
        found_user_id = true;
        // Remove user shard.
        std::string shard = sqlengine::NameShard(table_name, user_id);
        std::string path = absl::StrCat(state->dir_path(), shard, ".sqlite3");
        remove(path.c_str());
        state->RemoveUserFromShard(table_name, user_id);
        // Turn the delete statement back to a string, to delete relevant row in
        // PII table.
        std::string delete_str = stmt->accept(&stringify).as<std::string>();
        result.emplace_back(DEFAULT_SHARD_NAME, delete_str, identity_modifier);
      }
    }
    if (!found_user_id) {
      throw "DELETE PII statement does not specify an exact user to delete!";
    }
  }

  // Case 2: Table does not have PII and is not sharded!
  if (!is_sharded && !is_pii) {
    std::string delete_str = stmt->accept(&stringify).as<std::string>();
    result.emplace_back(DEFAULT_SHARD_NAME, delete_str, identity_modifier);
  }

  // Case 3: Table is sharded!
  if (is_sharded) {
    throw "Unsupported case for DELETE statement!";
  }

  return result;
}

}  // namespace delete_
}  // namespace sqlengine
}  // namespace shards
