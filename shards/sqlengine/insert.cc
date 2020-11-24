// INSERT statements sharding and rewriting.

#include "shards/sqlengine/insert.h"

#include <list>

#include "antlr4-runtime.h"
#include "shards/sqlengine/visitors/stringify.h"
#include "shards/sqlengine/util.h"

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

// Get the specific value from the insert statement at the given index in the
// value set.
std::string GetValueByIndex(
    sqlparser::SQLiteParser::Insert_stmtContext *stmt, size_t value_index) {
  // There must be only one set of values!
  sqlparser::SQLiteParser::Expr_listContext *values = stmt->expr_list(0);
  if (values->expr().size() < 2 || values->expr().size() <= value_index) {
    throw "Not enough values to insert into shard!";
  }
  
  return values->expr().at(value_index)->getText();
}

sqlparser::SQLiteParser::Insert_stmtContext *ShardStatement(
    sqlparser::SQLiteParser::Insert_stmtContext *stmt, size_t value_index,
    const std::string &sharded_table_name) {
  // There must be only one set of values!
  sqlparser::SQLiteParser::Expr_listContext *values = stmt->expr_list(0);
  if (values->expr().size() < 2 || values->expr().size() <= value_index) {
    throw "Not enough values to insert into shard!";
  }

  // Copy statement.
  // TODO(babman): improve copying?
  sqlparser::SQLiteParser::Insert_stmtContext *result =
      new sqlparser::SQLiteParser::Insert_stmtContext(
          static_cast<antlr4::ParserRuleContext *>(stmt->parent),
          stmt->invokingState);

  result->children = std::vector(stmt->children);
  for (uint32_t i = 0; i < result->children.size(); i++) {
    if (antlrcpp::is<sqlparser::SQLiteParser::Expr_listContext *>(result->children[i])) {
      result->children[i] =
          new sqlparser::SQLiteParser::Expr_listContext(
              static_cast<antlr4::ParserRuleContext *>(result),
              values->invokingState);
    }
  }

  // Remove the particular value.
  size_t i = 0;
  for (antlr4::tree::ParseTree *child : values->children) {
    if (antlrcpp::is<sqlparser::SQLiteParser::ExprContext *>(child)) {
      if (i++ == value_index) {
        continue;
      }
    }
    result->expr_list().at(0)->children.push_back(child);
  }

  // Modify name.
  antlr4::Token *symbol = result->table_name()->any_name()->IDENTIFIER()->getSymbol();
  *symbol = antlr4::CommonToken(symbol);
  static_cast<antlr4::CommonToken *>(symbol)->setText(sharded_table_name);

  return result;
}

}  // namespace

std::list<std::pair<std::string, std::string>> Rewrite(
    sqlparser::SQLiteParser::Insert_stmtContext *stmt, SharderState *state) {
  // Make sure table exists in the schema first.
  const std::string &table_name = stmt->table_name()->getText();
  if (!state->Exists(table_name)) {
    throw "Table does not exist!";
  }

  visitors::Stringify stringify;
  std::list<std::pair<std::string, std::string>> result;

  // Case 1: table is not in any shard.
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    // The insertion statement is unmodified.
    std::string insert_str = stmt->accept(&stringify).as<std::string>();
    result.push_back(std::make_pair(DEFAULT_SHARD_NAME, insert_str));
  }

  // Case 2: table is sharded!
  if (is_sharded) {
    // Duplicate the value for every shard this table has.
    for (const ShardingInformation &sharding_info : state->GetShardingInformation(table_name)) {
      // Find the value corresponding to the shard by column.
      ColumnIndex value_index = sharding_info.shard_by_index; 
      if (stmt->column_name().size() > 0) {
        value_index = FindColumnIndex(stmt, sharding_info.shard_by);
      }

      // Remove the value from the insert statement, and use it to detemine the
      // shard to execute the statement in.
      std::string value = GetValueByIndex(stmt, value_index);
      sqlparser::SQLiteParser::Insert_stmtContext *sharded_stmt =
          ShardStatement(stmt, value_index, sharding_info.sharded_table_name);
      std::string shard_name = NameShard(sharding_info.shard_kind, value);

      // Check if a shard was created for this user.
      // TODO(babman): better to do this after user insert rather than user data
      //               insert.
      if (!state->ShardExists(sharding_info.shard_kind, value)) {
        for (auto create_stmt : state->CreateShard(sharding_info.shard_kind, value)) {
          result.push_back(std::make_pair(shard_name, create_stmt));
        }
      }

      // Add the modified insert statement.
      std::string insert_stmt_str = sharded_stmt->accept(&stringify).as<std::string>();
      result.push_back(std::make_pair(shard_name, insert_stmt_str));
    }
  }
  return result;
}

}  // namespace insert
}  // namespace sqlengine
}  // namespace shards
