// SELECT statements sharding and rewriting.
#include "shards/sqlengine/select.h"

#include <utility>

// NOLINTNEXTLINE
#include "antlr4-runtime.h"
#include "shards/sqlengine/util.h"
#include "shards/sqlengine/visitors/stringify.h"

namespace shards {
namespace sqlengine {
namespace select {

namespace {

void RenameTable(sqlparser::SQLiteParser::Select_stmtContext *stmt,
                 const std::string &sharded_table_name) {
  auto *table_name = stmt->select_core(0)->table_or_subquery(0)->table_name();
  antlr4::Token *symbol = table_name->any_name()->IDENTIFIER()->getSymbol();
  *symbol = antlr4::CommonToken(symbol);
  static_cast<antlr4::CommonToken *>(symbol)->setText(sharded_table_name);
}

std::pair<sqlparser::SQLiteParser::ExprContext *, std::string>
FindAndRemoveExpr(sqlparser::SQLiteParser::ExprContext *expr,
                  const std::string &colname) {
  if (expr->column_name() != nullptr) {
    if (expr->column_name()->getText() == colname) {
      return std::make_pair(nullptr, "");
    }
    return std::make_pair(expr, "");
  }
  if (expr->literal_value() != nullptr) {
    return std::make_pair(expr, expr->literal_value()->getText());
  }
  auto *initial1 = expr->expr(0);
  auto *initial2 = expr->expr(1);
  auto [ptr1, str1] = FindAndRemoveExpr(initial1, colname);
  auto [ptr2, str2] = FindAndRemoveExpr(initial2, colname);
  if (ptr1 == nullptr) {
    if (expr->ASSIGN() != nullptr) {
      return std::make_pair(nullptr, str2);
    } else {
      return std::make_pair(ptr2, str1);
    }
  } else if (ptr2 == nullptr) {
    if (expr->ASSIGN() != nullptr) {
      return std::make_pair(nullptr, str1);
    } else {
      return std::make_pair(ptr1, str2);
    }
  }
  for (size_t i = 0; i < expr->children.size(); i++) {
    if (expr->children.at(i) == initial1) {
      expr->children.at(i) = ptr1;
    } else if (expr->children.at(i) == initial2) {
      expr->children.at(i) = ptr2;
    }
  }
  return std::make_pair(expr, str1.size() == 0 ? str1 : str2);
}

std::string FindAndRemoveWhere(
    sqlparser::SQLiteParser::Select_stmtContext *stmt,
    const std::string &colname) {
  if (stmt->select_core(0)->expr().size() < 1) {
    return "";
  }

  sqlparser::SQLiteParser::ExprContext *initial = stmt->select_core(0)->expr(0);
  auto [updated, str] = FindAndRemoveExpr(initial, colname);
  if (updated != initial) {
    auto &children = stmt->select_core(0)->children;
    for (size_t i = 0; i < children.size(); i++) {
      if (children.at(i) == initial) {
        children.at(i) = updated;
      }
    }
  }
  return str;
}

}  // namespace

std::list<std::tuple<std::string, std::string, CallbackModifier>> Rewrite(
    sqlparser::SQLiteParser::Select_stmtContext *stmt, SharderState *state) {
  visitors::Stringify stringify;
  std::list<std::tuple<std::string, std::string, CallbackModifier>> result;

  // Table name to select from.
  const std::string &table_name =
      stmt->select_core(0)->table_or_subquery(0)->table_name()->getText();

  // Case 1: table is not in any shard.
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    std::string select_str = stmt->accept(&stringify).as<std::string>();
    result.emplace_back(DEFAULT_SHARD_NAME, select_str, identity_modifier);
  }

  // Case 2: table is sharded.
  if (is_sharded) {
    // We will query all the different ways of sharding the table (for
    // duplicates with many owners), de-duplication occurs later in the
    // pipeline.
    for (const auto &info : state->GetShardingInformation(table_name)) {
      // Rename the table to match the sharded name.
      // TODO(babman): need a way to copy stmt so that side-effects from
      // different iterations are isolated.
      std::string user_id_val = FindAndRemoveWhere(stmt, info.shard_by);
      RenameTable(stmt, info.sharded_table_name);
      std::string select_str = stmt->accept(&stringify).as<std::string>();

      // Select from all the shards.
      char *user_id_col_name = new char[info.shard_by.size() + 1];
      memcpy(user_id_col_name, info.shard_by.c_str(), info.shard_by.size());
      user_id_col_name[info.shard_by.size()] = 0;
      int user_id_col_index = info.shard_by_index;

      for (const auto &user_id : state->UsersOfShard(info.shard_kind)) {
        if (user_id_val.size() > 0 && user_id != user_id_val) {
          continue;
        }

        char *user_id_data = new char[user_id.size() + 1];
        memcpy(user_id_data, user_id.c_str(), user_id.size());
        user_id_data[user_id.size()] = 0;

        std::string shard_name = NameShard(info.shard_kind, user_id);
        result.emplace_back(
            shard_name, select_str,
            [=](int *col_count, char ***col_data, char ***col_names) {
              (*col_count)++;
              char **new_data = new char *[*col_count];
              char **new_name = new char *[*col_count];
              new_data[user_id_col_index] = user_id_data;
              new_name[user_id_col_index] = user_id_col_name;
              for (int i = 0; i < user_id_col_index; i++) {
                new_data[i] = (*col_data)[i];
                new_name[i] = (*col_names)[i];
              }
              for (int i = user_id_col_index + 1; i < *col_count; i++) {
                new_data[i] = (*col_data)[i - 1];
                new_name[i] = (*col_names)[i - 1];
              }
              *col_data = new_data;
              *col_names = new_name;
              identity_modifier(col_count, col_data, col_names);
            });
      }
    }
  }

  return result;
}

}  // namespace select
}  // namespace sqlengine
}  // namespace shards
