// CREATE TABLE statements sharding and rewriting.

#include "shards/sqlengine/create.h"

#include <list>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
// NOLINTNEXTLINE
#include "antlr4-runtime.h"
#include "shards/sqlengine/util.h"
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
  // A foreign key to the PII table that is the shard_by column:
  // both column and constraint are removed, since column value is redundant.
  FK_REMOVED
};

// Describes what should happen to every foreign key in a given table.
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

// Checks if this foreign key can be used to shard its table.
// Specifically, if this is a foreign key to a PII table or a table that is
// itself sharded.
std::optional<ShardKind> ShouldShardBy(
    sqlparser::SQLiteParser::Foreign_key_clauseContext *foreign_key,
    const SharderState &state) {
  // First, determine if the foreign table is in a shard or has PII.
  UnshardedTableName foreign_table = foreign_key->foreign_table()->getText();
  bool foreign_sharded = state.IsSharded(foreign_table);
  bool foreign_pii = state.IsPII(foreign_table);

  // Our insert rewriting logic does not support this case yet!
  // Transitive fk sharding.
  if (foreign_sharded) {
    throw "Transitive sharding is not supported!";
  }

  // FK links to a shard, add it as either an explicit or implicit owner.
  if (foreign_pii) {
    return foreign_table;
  }
  return {};
}

// Figures out which shard kind this table should belong to.
// Currently, we expect one of two solutions:
// 1) a foreign key column is explicitly specified to be the column to shard by
//    via OWNER annotation.
// 2) if no OWNER is specified, we can deduce automatically what to shard by (if
//    at all). Currently, we support only a single foreign key to be linked
//    to a PII table or to another table in some shard. This table is assigned
//    to the shard defined by that PII table, or to the same shard that the
//    target table is in. Having multiple such foreign keys results in a runtime
//    error.
std::list<ShardingInformation> ShardTable(
    sqlparser::SQLiteParser::Create_table_stmtContext *stmt,
    const SharderState &state) {
  // Result is empty by default.
  std::list<ShardingInformation> explicit_owners;
  std::list<ShardingInformation> implicit_owners;
  std::unordered_map<ColumnName, ColumnIndex> index_map;

  const std::string &table_name = stmt->table_name()->getText();
  // Check column definitions for inlined foreign key constraints.
  auto columns = stmt->column_def();
  for (size_t index = 0; index < columns.size(); index++) {
    // Record index of column for later constraint processing.
    ColumnName column_name = columns[index]->column_name()->getText();
    index_map.insert({column_name, index});
    // Find foreign key constraint (if exists).
    for (auto *constraint : columns[index]->column_constraint()) {
      auto *foreign_key = constraint->foreign_key_clause();
      if (foreign_key == nullptr) {
        continue;
      }

      // We have a foreign key constraint, check if we should shard by it.
      bool explicit_owner = absl::StartsWith(column_name, "OWNER_");
      std::optional<ShardKind> shard_kind = ShouldShardBy(foreign_key, state);
      if (shard_kind.has_value()) {
        std::string sharded_table_name =
            NameShardedTable(table_name, column_name);
        // FK links to a shard, add it as either an explicit or implicit owner.
        if (explicit_owner) {
          explicit_owners.push_back(
              {shard_kind.value(), sharded_table_name, column_name, index});
        } else {
          implicit_owners.push_back(
              {shard_kind.value(), sharded_table_name, column_name, index});
        }
      }
    }
  }

  // Check standalone table constraints.
  for (auto *constraint : stmt->table_constraint()) {
    auto *foreign_key = constraint->foreign_key_clause();
    if (foreign_key == nullptr) {
      continue;
    }

    // See if we should shard by this foreign key.
    std::optional<ShardKind> shard_kind = ShouldShardBy(foreign_key, state);
    if (shard_kind.has_value()) {
      if (constraint->column_name().size() > 1) {
        throw "Multi-column foreign key shard link are not supported!";
      }

      ColumnName column_name = constraint->column_name(0)->getText();
      std::string sharded_table_name =
          NameShardedTable(table_name, column_name);
      ColumnIndex column_index = index_map.at(column_name);
      bool explicit_owner = absl::StartsWith(column_name, "OWNER_");
      if (explicit_owner) {
        explicit_owners.push_back({shard_kind.value(), sharded_table_name,
                                   column_name, column_index});
      } else {
        implicit_owners.push_back({shard_kind.value(), sharded_table_name,
                                   column_name, column_index});
      }
    }
  }

  // If sharding is explicitly specified via OWNER, we are done.
  if (explicit_owners.size() > 0) {
    return explicit_owners;
  }

  // Sharding is implicitly deduced, only works if there is one candidate!
  if (implicit_owners.size() > 1) {
    throw "Table with several foreign keys to shards or PII!";
  }
  return implicit_owners;
}

// Determine what hsould be done about a single foreign key in some
// sharded table.
ForeignKeyType ShardForeignKey(
    sqlparser::SQLiteParser::Foreign_key_clauseContext *foreign_key,
    const ShardingInformation &sharding_information,
    const SharderState &state) {
  const std::string &foreign_table = foreign_key->foreign_table()->getText();

  // Figure out the type of this foreign key.
  if (state.IsSharded(foreign_table)) {
    const std::list<ShardingInformation> &list =
        state.GetShardingInformation(foreign_table);
    if (list.size() > 0) {
      throw "Unsupported: transitive foreign key into multi-owned table!";
    }
    const ShardKind &target = list.front().shard_kind;
    if (target == sharding_information.shard_kind) {
      return FK_INTERNAL;
    }
  }
  return FK_EXTERNAL;
}

// Determine what should be done about all foreign keys in a sharded table.
ForeignKeyShards ShardForeignKeys(
    sqlparser::SQLiteParser::Create_table_stmtContext *stmt,
    const ShardingInformation &sharding_information,
    const SharderState &state) {
  // Store the sharded resolution for every foreign key.
  ForeignKeyShards result;
  // Check column definitions for inlined foreign key constraints.
  for (auto *column : stmt->column_def()) {
    for (auto *constraint : column->column_constraint()) {
      auto *foreign_key = constraint->foreign_key_clause();
      if (foreign_key != nullptr) {
        result.emplace(
            column->column_name()->getText(),
            ShardForeignKey(foreign_key, sharding_information, state));
      }
    }
  }
  // Check standalone table constraints.
  for (auto *constraint : stmt->table_constraint()) {
    auto *foreign_key = constraint->foreign_key_clause();
    if (foreign_key != nullptr) {
      ForeignKeyType type =
          ShardForeignKey(foreign_key, sharding_information, state);
      for (auto *column_name : constraint->column_name()) {
        result.emplace(column_name->getText(), type);
      }
    }
  }
  // Remove shard_by column and its constraints.
  result[sharding_information.shard_by] = FK_REMOVED;
  return result;
}

// Removes any foreign key constraints attached to this column definition.
sqlparser::SQLiteParser::Column_defContext *RemoveForeignKeyConstraint(
    sqlparser::SQLiteParser::Column_defContext *def) {
  // Make copy of column definition.
  // TODO(babman): improve copying?
  sqlparser::SQLiteParser::Column_defContext *result =
      new sqlparser::SQLiteParser::Column_defContext(
          static_cast<antlr4::ParserRuleContext *>(def->parent),
          def->invokingState);

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
    result->children.push_back(child);
  }
  return result;
}

// Drops the given column (by name) from the create table statement along
// with all its constraints.
sqlparser::SQLiteParser::Create_table_stmtContext *UpdateTableSchema(
    sqlparser::SQLiteParser::Create_table_stmtContext *stmt,
    const ForeignKeyShards &fk_shards, const std::string &sharded_table_name) {
  // Copy stmt.
  // TODO(babman): improve copying?
  sqlparser::SQLiteParser::Create_table_stmtContext *result =
      new sqlparser::SQLiteParser::Create_table_stmtContext(
          static_cast<antlr4::ParserRuleContext *>(stmt->parent),
          stmt->invokingState);

  for (antlr4::tree::ParseTree *child : stmt->children) {
    // Drop matching column definition.
    if (antlrcpp::is<sqlparser::SQLiteParser::Column_defContext *>(child)) {
      sqlparser::SQLiteParser::Column_defContext *column_def =
          dynamic_cast<sqlparser::SQLiteParser::Column_defContext *>(child);
      const std::string &column_name = column_def->column_name()->getText();
      if (fk_shards.count(column_name) == 1) {
        ForeignKeyType type = fk_shards.at(column_name);
        switch (type) {
          case FK_REMOVED:
            goto drop_child;
          case FK_EXTERNAL:
            column_def = RemoveForeignKeyConstraint(column_def);
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
        const std::string &column_name = constraint_column->getText();
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
        const std::string &column_name = constraint_column->getText();
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
    result->children.push_back(child);
  // NOLINTNEXTLINE
  drop_child:;  // Child should be completely removed.
  }

  // Change table name.
  // TODO(babman): improve copying?
  antlr4::Token *symbol =
      result->table_name()->any_name()->IDENTIFIER()->getSymbol();
  *symbol = antlr4::CommonToken(symbol);
  static_cast<antlr4::CommonToken *>(symbol)->setText(sharded_table_name);

  return result;
}

}  // namespace

std::list<std::tuple<std::string, std::string, CallbackModifier>> Rewrite(
    sqlparser::SQLiteParser::Create_table_stmtContext *stmt,
    SharderState *state) {
  const std::string &table_name = stmt->table_name()->getText();
  if (state->Exists(table_name)) {
    throw "Table already exists!";
  }

  // Determine if this table is special: maybe it has PII fields, or maybe it
  // is linked to an existing shard via a foreign key.
  bool has_pii = HasPII(stmt);
  std::list<ShardingInformation> sharding_information =
      ShardTable(stmt, *state);
  if (has_pii && sharding_information.size() > 0) {
    throw "Sharded Table cannot have PII fields!";
  }

  visitors::Stringify stringify;
  std::list<std::tuple<std::string, std::string, CallbackModifier>> result;
  // Case 1: has pii but not linked to shards.
  // This means that this table define a type of user for which shards must be
  // created! Hence, it is a shard kind!
  if (has_pii && sharding_information.size() == 0) {
    std::string create_table_str = stmt->accept(&stringify).as<std::string>();
    state->AddShardKind(table_name);
    state->AddUnshardedTable(table_name, create_table_str);
    result.emplace_back(DEFAULT_SHARD_NAME, create_table_str,
                        identity_modifier);
  }

  // Case 2: no pii but is linked to shards.
  // This means that this table should be created inside shards of the kind it
  // is linked to. We must also drop the foreign key column(s) that link it to
  // the shard.
  if (!has_pii && sharding_information.size() > 0) {
    for (const auto &info : sharding_information) {
      // Determine what to do to each column that is a foreign key.
      ForeignKeyShards fk_shards = ShardForeignKeys(stmt, info, *state);
      // Apply sharding and come up with new schema.
      sqlparser::SQLiteParser::Create_table_stmtContext *sharded_stmt =
          UpdateTableSchema(stmt, fk_shards, info.sharded_table_name);
      std::string create_table_str =
          sharded_stmt->accept(&stringify).as<std::string>();
      // Add the sharding information to state.
      state->AddShardedTable(table_name, info, create_table_str);
    }
  }

  // Case 3: neither pii nor linked.
  // The table does not belong to a shard and needs no further modification!
  if (!has_pii && sharding_information.size() == 0) {
    std::string create_table_str = stmt->accept(&stringify).as<std::string>();
    state->AddUnshardedTable(table_name, create_table_str);
    result.emplace_back(DEFAULT_SHARD_NAME, create_table_str,
                        identity_modifier);
  }

  return result;
}

}  // namespace create
}  // namespace sqlengine
}  // namespace shards
