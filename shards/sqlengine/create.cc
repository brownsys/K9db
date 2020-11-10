// CREATE TABLE statements sharding and rewriting.

#include "shards/sqlengine/create.h"

#include <iostream>
#include <list>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
// NOLINTNEXTLINE
#include "antlr4-runtime.h"
#include "shards/sqlengine/visitors/stringify.h"

namespace shards {
namespace sqlengine {
namespace create {

namespace {

// Describes whether a foreign key is internal to a shard (between two tables
// of the same shard kind) or external (the two tables belong to different
// shard kinds).
enum ForeignKeyType {
  // External: the column is kept, but the foreign key constraint is removed.
  FK_EXTERNAL,
  // Internal: column and constraint are unchanged.
  FK_INTERNAL,
  // A foreign key to the PII table that is the basis of the shard:
  // both column and constraint are removed, column value is redundant.
  FK_REMOVED
};

// Describes what a specific foreign key looks in the sharded schema!
using ForeignKeyShards = std::unordered_map<ColumnName, ForeignKeyType>;

// Checks if the create table statement specifies any PII columns.
bool HasPII(sqlparser::SQLiteParser::Create_table_stmtContext *stmt) {
  for (sqlparser::SQLiteParser::Column_defContext *column :
       stmt->column_def()) {
    if (absl::StartsWith(column->column_name()->getText(), "PII_")) {
      return true;
    }
  }
  return false;
}

// Figures out which shard kind this table should belong to.
// Currently, we expect only a single foreign key to be linked to a PII table
// or to another table in some shard. This table is assigned to the shard
// defined by that PII table, or to the same shard that the target table is in.
// Having multiple such foreign keys results in a runtime error.
std::optional<std::tuple<ColumnName, ColumnIndex, ShardKind>>
DetermineShardKind(sqlparser::SQLiteParser::Create_table_stmtContext *stmt,
                   SharderState *state) {
  // Result is none by default.
  std::optional<std::tuple<ColumnName, ColumnIndex, ShardKind>> result;
  std::unordered_map<ColumnName, ColumnIndex> index_map;
  // Check column definitions for inlined foreign key constraints.
  int index = 0;
  for (auto *column : stmt->column_def()) {
    ColumnName column_name = column->column_name()->getText();
    index_map.insert({column_name, index});
    for (auto *constraint : column->column_constraint()) {
      auto *foreign_key = constraint->foreign_key_clause();
      if (foreign_key != nullptr) {
        TableName foreign_table = foreign_key->foreign_table()->getText();
        std::optional<ShardKind> foreign_shard =
            state->ShardKindOf(foreign_table);
        bool foreign_pii = state->IsPII(foreign_table);
        if (foreign_pii || foreign_shard.has_value()) {
          if (result.has_value()) {
            throw "Table with several foreign keys to shards or PII!";
          }
          if (foreign_pii) {
            result = std::make_tuple(column_name, index, foreign_table);
          }
          if (foreign_shard.has_value()) {
            // Our insert rewriting logic does not support this case yet!
            throw "Indirect sharding is not supported!";
            result = std::make_tuple(column_name, index, foreign_shard.value());
          }
        }
      }
    }
    index++;
  }
  // Check standalone table constraints.
  for (auto *constraint : stmt->table_constraint()) {
    auto *foreign_key_clause = constraint->foreign_key_clause();
    if (foreign_key_clause != nullptr) {
      if (constraint->column_name().size() > 1) {
        throw "Multi-column foreign key shard link are not supported!";
      }

      TableName foreign_table = foreign_key_clause->foreign_table()->getText();
      std::optional<ShardKind> foreign_shard =
          state->ShardKindOf(foreign_table);
      bool foreign_pii = state->IsPII(foreign_table);
      if (foreign_pii || foreign_shard.has_value()) {
        if (result.has_value()) {
          throw "Table with several foreign keys to shards or PII!";
        }

        ColumnName column_name = constraint->column_name(0)->getText();
        ColumnIndex column_index = index_map.at(column_name);
        if (foreign_pii) {
          result = std::make_tuple(column_name, column_index, foreign_table);
        }
        if (foreign_shard.has_value()) {
          // Our insert rewriting logic does not support this case yet!
          throw "Indirect sharding is not supported!";
          result =
              std::make_tuple(column_name, column_index, foreign_shard.value());
        }
      }
    }
  }
  return result;
}

// Determine what should be done about foreign keys in a table designated
// to be sharded!
ForeignKeyShards ShardForeignKeys(
    sqlparser::SQLiteParser::Create_table_stmtContext *stmt,
    const ShardKind &shard_kind, SharderState *state) {
  // Store the sharded resolution for every foreign key.
  ForeignKeyShards result;
  // Check column definitions for inlined foreign key constraints.
  for (auto *column : stmt->column_def()) {
    for (auto *constraint : column->column_constraint()) {
      auto *foreign_key = constraint->foreign_key_clause();
      if (foreign_key != nullptr) {
        std::string foreign_table = foreign_key->foreign_table()->getText();
        std::optional<ShardKind> target = state->ShardKindOf(foreign_table);
        // Figure out the type of this foreign key.
        ForeignKeyType type = FK_EXTERNAL;
        if (target.has_value() && target.value() == shard_kind) {
          type = FK_INTERNAL;
        }
        result.emplace(column->column_name()->getText(), type);
      }
    }
  }
  // Check standalone table constraints.
  for (auto *constraint : stmt->table_constraint()) {
    auto *foreign_key = constraint->foreign_key_clause();
    if (foreign_key != nullptr) {
      std::string foreign_table = foreign_key->foreign_table()->getText();
      std::optional<ShardKind> target = state->ShardKindOf(foreign_table);
      // Figure out the type of this foreign key.
      ForeignKeyType type = FK_EXTERNAL;
      if (target.has_value() && target.value() == shard_kind) {
        type = FK_INTERNAL;
      }
      for (auto *column_name : constraint->column_name()) {
        result.emplace(column_name->getText(), type);
      }
    }
  }
  return result;
}

// Removes any foreign key constraints attached to this column definition.
void RemoveForeignKeyConstraint(
    sqlparser::SQLiteParser::Column_defContext *def) {
  std::vector<antlr4::tree::ParseTree *> remaining_children;
  for (antlr4::tree::ParseTree *child : def->children) {
    if (antlrcpp::is<sqlparser::SQLiteParser::Column_constraintContext *>(
            child)) {
      sqlparser::SQLiteParser::Column_constraintContext *constraint =
          dynamic_cast<sqlparser::SQLiteParser::Column_constraintContext *>(
              child);
      if (constraint->foreign_key_clause() != nullptr) {
        continue;
      }
    }
    remaining_children.push_back(child);
  }
  def->children = remaining_children;
}

// Drops the given column (by name) from the create table statement along
// with all its constraints.
void UpdateTableSchema(sqlparser::SQLiteParser::Create_table_stmtContext *stmt,
                       const ForeignKeyShards &fk_shards) {
  std::vector<antlr4::tree::ParseTree *> remaining_children;
  for (antlr4::tree::ParseTree *child : stmt->children) {
    // Drop matching column definition.
    if (antlrcpp::is<sqlparser::SQLiteParser::Column_defContext *>(child)) {
      sqlparser::SQLiteParser::Column_defContext *column_def =
          dynamic_cast<sqlparser::SQLiteParser::Column_defContext *>(child);
      std::string column_name = column_def->column_name()->getText();
      if (fk_shards.count(column_name) == 1) {
        ForeignKeyType type = fk_shards.at(column_name);
        switch (type) {
          case FK_REMOVED:
            goto drop_child;
          case FK_EXTERNAL:
            RemoveForeignKeyConstraint(column_def);
          case FK_INTERNAL:
            goto keep_child;
        }
      }
    }
    // Remove certain constraints defined over certain columns per fk_shards!
    if (antlrcpp::is<sqlparser::SQLiteParser::Table_constraintContext *>(
            child)) {
      // TODO(babman): a constraint spanning several columns is currently
      // dropped even when only one/some of them are to be dropped.
      // This may not be desired for unique and primary key constraints.
      sqlparser::SQLiteParser::Table_constraintContext *constraint =
          dynamic_cast<sqlparser::SQLiteParser::Table_constraintContext *>(
              child);
      // UNIQUE / PRIMARY KEY constraints: only removed if the column is to be
      // removed completely, otherwise unchaged.
      for (auto *constraint_column : constraint->indexed_column()) {
        std::string column_name = constraint_column->getText();
        if (fk_shards.count(column_name) == 1) {
          ForeignKeyType type = fk_shards.at(column_name);
          switch (type) {
            case FK_REMOVED:
              goto drop_child;
            case FK_EXTERNAL:
              break;
            case FK_INTERNAL:
              break;
          }
        }
      }
      // FOREIGN KEY constraints: removed if the column is to be removed
      // or if the constraint is external, otherwise kept unchanged!
      for (auto *constraint_column : constraint->column_name()) {
        std::string column_name = constraint_column->getText();
        if (fk_shards.count(column_name) == 1) {
          ForeignKeyType type = fk_shards.at(column_name);
          switch (type) {
            case FK_REMOVED:
              goto drop_child;
            case FK_EXTERNAL:
              goto drop_child;
            case FK_INTERNAL:
              break;
          }
        }
      }
    }
  // Yeah, a GOTO statement, sue me...
  // NOLINTNEXTLINE
  keep_child:;  // Child should not be dropped, but might be modified.
    remaining_children.push_back(child);
  // NOLINTNEXTLINE
  drop_child:;  // Child should be completely removed.
  }

  stmt->children = remaining_children;
}

}  // namespace

std::list<std::pair<std::string, std::string>> Rewrite(
    sqlparser::SQLiteParser::Create_table_stmtContext *stmt,
    SharderState *state) {
  std::string table_name = stmt->table_name()->getText();
  if (state->Exists(table_name)) {
    throw "Table already exists!";
  }

  // Determine if this table is special: maybe it has PII fields, or maybe it
  // is linked to an existing shard via a foreign key.
  bool has_pii = HasPII(stmt);
  std::optional<std::tuple<ColumnName, ColumnIndex, ShardKind>> linked =
      DetermineShardKind(stmt, state);
  if (has_pii && linked.has_value()) {
    throw "Sharded Table cannot have PII fields!";
  }

  visitors::Stringify stringify;
  std::list<std::pair<std::string, std::string>> result;
  // Case 1: has pii but not linked to shards.
  // This means that this table define a type of user for which shards must be
  // created! Hence, it is a shard kind!
  if (has_pii && !linked.has_value()) {
    std::string create_table_str = stmt->accept(&stringify).as<std::string>();
    state->AddShardKind(table_name);
    state->AddUnshardedTable(table_name, create_table_str);
    result.push_back(std::make_pair("default", create_table_str));
  }

  // Case 2: no pii but is linked to shards.
  // This means that this table should be created inside shards of the kind it
  // is linked to. We must also drop the foreign key column(s) that link it to
  // the shard.
  if (!has_pii && linked.has_value()) {
    auto [column_name, column_index, shard_kind] = linked.value();
    ForeignKeyShards fk_shards = ShardForeignKeys(stmt, shard_kind, state);
    fk_shards[column_name] = FK_REMOVED;
    UpdateTableSchema(stmt, fk_shards);
    std::string create_table_str = stmt->accept(&stringify).as<std::string>();
    state->AddShardedTable(table_name, shard_kind,
                           std::make_pair(column_name, column_index),
                           create_table_str);
  }

  // Case 3: neither pii nor linked.
  // The table does not belong to a shard and needs no further modification!
  if (!has_pii && !linked.has_value()) {
    std::string create_table_str = stmt->accept(&stringify).as<std::string>();
    state->AddUnshardedTable(table_name, create_table_str);
    result.push_back(std::make_pair("default", create_table_str));
  }

  return result;
}

}  // namespace create
}  // namespace sqlengine
}  // namespace shards
