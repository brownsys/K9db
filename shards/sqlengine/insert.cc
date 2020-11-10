// INSERT statements sharding and rewriting.

#include "shards/sqlengine/insert.h"

#include <list>

// NOLINTNEXTLINE
#include "absl/strings/str_format.h"
#include "antlr4-runtime.h"
#include "shards/sqlengine/visitors/stringify.h"

namespace shards {
namespace sqlengine {
namespace insert {

namespace {

// Find the index in the insert statement column list where the given column
// appears.
size_t FindColumnIndex(sqlparser::SQLiteParser::Insert_stmtContext *stmt,
                       std::string column_name) {
  for (size_t i = 0; i < stmt->column_name().size(); i++) {
    if (stmt->column_name(i)->getText() == column_name) {
      return i;
    }
  }
  throw "Cannot find sharding column in insert statement!";
}

// Remove the value at the given index from the statement (in place) and
// return that value.
std::string RemoveValueByIndex(
    sqlparser::SQLiteParser::Insert_stmtContext *stmt, size_t value_index) {
  // There is only one set of values!
  sqlparser::SQLiteParser::Expr_listContext *values = stmt->expr_list(0);
  if (values->expr().size() < 2 || values->expr().size() <= value_index) {
    throw "Not enough values to insert into shard!";
  }

  // Remove the particular value.
  std::vector<antlr4::tree::ParseTree *> remaining_children;
  size_t i = 0;
  std::string value;
  for (antlr4::tree::ParseTree *child : values->children) {
    if (antlrcpp::is<sqlparser::SQLiteParser::ExprContext *>(child)) {
      if (i++ == value_index) {
        value = child->getText();
        continue;
      }
    }
    remaining_children.push_back(child);
  }

  values->children = remaining_children;
  return value;
}

}  // namespace

std::list<std::pair<std::string, std::string>> Rewrite(
    sqlparser::SQLiteParser::Insert_stmtContext *stmt, SharderState *state) {
  const std::string &table_name = stmt->table_name()->getText();
  if (!state->Exists(table_name)) {
    throw "Table does not exist!";
  }

  visitors::Stringify stringify;
  std::list<std::pair<std::string, std::string>> result;
  std::optional<ShardKind> shard_kind = state->ShardKindOf(table_name);

  // Case 1: table is not in any shard.
  if (!shard_kind.has_value()) {
    std::string insert_str = stmt->accept(&stringify).as<std::string>();
    result.push_back(std::make_pair("default", insert_str));
  }

  // Case 2: table is sharded!
  if (shard_kind.has_value()) {
    // Find sharding value (the user id).
    auto [column_name, column_index] = state->ShardedBy(table_name).value();
    size_t value_index = column_index;
    if (stmt->column_name().size() > 0) {
      value_index = FindColumnIndex(stmt, column_name);
    }
    std::string value = RemoveValueByIndex(stmt, value_index);
    std::string shard_name =
        absl::StrFormat("%s_%s", shard_kind.value(), value);

    // Check if a shard was created for this user.
    if (!state->ShardExists(shard_kind.value(), value)) {
      for (auto create_stmt : state->CreateShard(shard_kind.value(), value)) {
        result.push_back(std::make_pair(shard_name, create_stmt));
      }
    }
    result.push_back(
        std::make_pair(shard_name, stmt->accept(&stringify).as<std::string>()));
  }
  return result;
}

}  // namespace insert
}  // namespace sqlengine
}  // namespace shards
