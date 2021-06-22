// Defines the state of our sharding adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#include "pelton/shards/state.h"

#include <stdlib.h>
#include <string.h>

#include <utility>

namespace pelton {
namespace shards {

// Initialization.
void SharderState::Initialize(const std::string &db_username,
                              const std::string &db_password) {
  this->connection_pool_.Initialize(db_username, db_password);
}

// Schema manipulations.
void SharderState::AddSchema(const UnshardedTableName &table_name,
                             const sqlast::CreateTable &table_schema) {
  this->schema_.insert({table_name, table_schema});
}

void SharderState::AddShardKind(const ShardKind &kind, const ColumnName &pk) {
  this->kinds_.insert({kind, pk});
  this->kind_to_tables_.insert({kind, {}});
  this->kind_to_names_.insert({kind, {}});
  this->shards_.insert({kind, {}});
}

void SharderState::AddUnshardedTable(const UnshardedTableName &table,
                                     const sqlast::CreateTable &create) {
  this->sharded_schema_.insert({table, create});
}

void SharderState::AddShardedTable(
    const UnshardedTableName &table,
    const ShardingInformation &sharding_information,
    const sqlast::CreateTable &sharded_create_statement) {
  // Record that the shard kind contains this sharded table.
  this->kind_to_tables_.at(sharding_information.shard_kind)
      .push_back(sharding_information.sharded_table_name);
  this->kind_to_names_.at(sharding_information.shard_kind).insert(table);
  // Map the unsharded name to its sharding information.
  this->sharded_by_[table].push_back(sharding_information);
  // Store the sharded schema.
  this->sharded_schema_.insert(
      {sharding_information.sharded_table_name, sharded_create_statement});
}

std::list<const sqlast::AbstractStatement *> SharderState::CreateShard(
    const ShardKind &shard_kind, const UserId &user) {
  // Mark shard for this user as created!
  this->shards_.at(shard_kind).insert(user);
  // Return the create table statements.
  std::list<const sqlast::AbstractStatement *> result;
  for (const ShardedTableName &table : this->kind_to_tables_.at(shard_kind)) {
    result.push_back(&this->sharded_schema_.at(table));
  }
  for (const sqlast::CreateIndex &create_index :
       this->create_index_[shard_kind]) {
    result.push_back(&create_index);
  }
  return result;
}

void SharderState::RemoveUserFromShard(const ShardKind &kind,
                                       const UserId &user_id) {
  this->shards_.at(kind).erase(user_id);
}

// Schema lookups.
const sqlast::CreateTable &SharderState::GetSchema(
    const UnshardedTableName &table_name) const {
  return this->schema_.at(table_name);
}

bool SharderState::Exists(const UnshardedTableName &table) const {
  return this->sharded_schema_.count(table) > 0 ||
         this->sharded_by_.count(table) > 0;
}

bool SharderState::IsSharded(const UnshardedTableName &table) const {
  return this->sharded_by_.count(table) == 1;
}

const std::list<ShardingInformation> &SharderState::GetShardingInformation(
    const UnshardedTableName &table) const {
  return this->sharded_by_.at(table);
}

bool SharderState::IsPII(const UnshardedTableName &table) const {
  return this->kinds_.count(table) > 0;
}

const ColumnName &SharderState::PkOfPII(const UnshardedTableName &table) const {
  return this->kinds_.at(table);
}

bool SharderState::ShardExists(const ShardKind &shard_kind,
                               const UserId &user) const {
  return this->shards_.at(shard_kind).count(user) > 0;
}

const std::unordered_set<UserId> &SharderState::UsersOfShard(
    const ShardKind &kind) const {
  return this->shards_.at(kind);
}

const std::unordered_set<UnshardedTableName> &SharderState::TablesInShard(
    const ShardKind &kind) const {
  return this->kind_to_names_.at(kind);
}

// Manage secondary indices.
bool SharderState::HasIndexFor(const UnshardedTableName &table_name,
                               const ColumnName &column_name,
                               const ColumnName &shard_by) const {
  if (this->index_to_flow_.count(table_name) == 0) {
    return false;
  }

  auto &tbl = this->index_to_flow_.at(table_name);
  if (this->index_to_flow_.at(table_name).count(column_name) == 0) {
    return false;
  }

  auto &col = tbl.at(column_name);
  return col.count(shard_by) > 0;
}

const std::unordered_set<ColumnName> &SharderState::IndicesFor(
    const UnshardedTableName &table_name) {
  return this->indices_[table_name];
}

const FlowName &SharderState::IndexFlow(const UnshardedTableName &table_name,
                                        const ColumnName &column_name,
                                        const ColumnName &shard_by) const {
  return this->index_to_flow_.at(table_name).at(column_name).at(shard_by);
}

void SharderState::CreateIndex(const ShardKind &shard_kind,
                               const UnshardedTableName &table_name,
                               const ColumnName &column_name,
                               const ColumnName &shard_by,
                               const FlowName &flow_name,
                               const sqlast::CreateIndex &create_index_stmt,
                               bool unique) {
  this->indices_[table_name].insert(column_name);
  this->index_to_flow_[table_name][column_name][shard_by] = flow_name;
  if (!unique) {
    this->create_index_[shard_kind].push_back(create_index_stmt);
  }
}

}  // namespace shards
}  // namespace pelton
