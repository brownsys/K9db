// Defines the state of our adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#include "shards/state.h"

#include <stdlib.h>
#include <string.h>

#include <iostream>
#include <utility>

namespace shards {

// Initialization.
void SharderState::Initialize(const std::string &dir_path) {
  this->dir_path_ = dir_path;
  if (dir_path.back() != '/') {
    this->dir_path_ += "/";
  }
  this->pool_.Initialize(this->dir_path_);
}

// Schema manipulations.
void SharderState::AddShardKind(const ShardKind &kind) {
  this->kinds_.insert(kind);
  this->kind_to_tables_.insert({kind, {}});
  this->shards_.insert({kind, {}});
}

void SharderState::AddUnshardedTable(const TableName &table,
                                     const CreateStatement &create_statement) {
  this->schema_.insert({table, create_statement});
}

void SharderState::AddShardedTable(
    const TableName &table, const ShardKind &kind,
    const std::pair<ColumnName, ColumnIndex> &shard_by,
    const CreateStatement &create_statement) {
  this->kind_to_tables_.at(kind).push_back(table);
  this->table_to_kind_.insert({table, kind});
  this->sharded_by_.insert({table, shard_by});
  this->schema_.insert({table, create_statement});
}

std::list<CreateStatement> SharderState::CreateShard(
    const ShardKind &shard_kind, const UserId &user) {
  // Mark shard as created!
  this->shards_.at(shard_kind).insert(user);
  // Return the create table statements.
  std::list<CreateStatement> result;
  for (const TableName &table_name : this->kind_to_tables_.at(shard_kind)) {
    result.push_back(this->schema_.at(table_name));
  }
  return result;
}

// Schema lookups.
bool SharderState::Exists(const TableName &table) const {
  return this->schema_.count(table) > 0;
}

std::optional<ShardKind> SharderState::ShardKindOf(
    const TableName &table) const {
  if (this->table_to_kind_.count(table) == 1) {
    return this->table_to_kind_.at(table);
  }
  return {};
}

std::optional<std::pair<ColumnName, ColumnIndex>> SharderState::ShardedBy(
    const TableName &table) const {
  if (this->sharded_by_.count(table) == 1) {
    return this->sharded_by_.at(table);
  }
  return {};
}

bool SharderState::IsPII(const TableName &table) const {
  return this->kinds_.count(table) > 0;
}

bool SharderState::ShardExists(const ShardKind &shard_kind,
                               const UserId &user) const {
  return this->shards_.at(shard_kind).count(user) > 0;
}

// Sqlite3 Connection pool interace.
bool SharderState::ExecuteStatement(const std::string &shard_suffix,
                                    const std::string &sql_statement) {
  return this->pool_.ExecuteStatement(shard_suffix, sql_statement);
}

}  // namespace shards
