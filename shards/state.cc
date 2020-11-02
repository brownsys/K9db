// Defines the state of our adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#include "shards/state.h"

#include <stdlib.h>
#include <string.h>

#include <utility>

namespace shards {

// Initialization.
void SharderState::Initialize(const std::string &dir_path) {
  this->dir_path_ = dir_path;
}

// Schema manipulations.
void SharderState::AddShardKind(const ShardKind &kind) {
  this->kinds_.insert(kind);
  this->kind_to_tables_.insert({kind, {}});
}

void SharderState::AddUnshardedTable(const TableName &table,
                                     const CreateStatement &create_statement) {
  this->schema_.insert({table, create_statement});
}

void SharderState::AddShardedTable(const TableName &table,
                                   const ShardKind &kind,
                                   const CreateStatement &create_statement) {
  this->kind_to_tables_.at(kind).insert(table);
  this->table_to_kind_.insert({table, kind});
  this->schema_.insert({table, create_statement});
}

void SharderState::AddTablePrimaryKeys(const TableName &table,
                                       std::unordered_set<ColumnName> &&keys) {
  this->primary_keys_.emplace(table, std::move(keys));
}

// Schema lookups.
bool SharderState::Exists(const TableName &table) const {
  return this->schema_.count(table) > 0;
}

bool SharderState::ExistsInShardKind(const ShardKind &shard,
                                     const TableName &table) const {
  return this->kind_to_tables_.at(shard).count(table) == 1;
}

std::optional<ShardKind> SharderState::ShardKindOf(
    const TableName &table) const {
  if (this->table_to_kind_.count(table) == 1) {
    return this->table_to_kind_.at(table);
  }
  return {};
}

bool SharderState::IsPII(const TableName &table) const {
  return this->kinds_.count(table) > 0;
}

const std::unordered_set<ColumnName> &SharderState::TableKeys(
    const TableName &table) const {
  return this->primary_keys_.at(table);
}

}  // namespace shards
