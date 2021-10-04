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
// writes to executor_ (w1)
void SharderState::Initialize(const std::string &db_name,
                              const std::string &db_username,
                              const std::string &db_password) {
  this->executor_.Initialize(db_name, db_username, db_password);
}

// Schema manipulations.
// writes to schema_ (w1)
void SharderState::AddSchema(const UnshardedTableName &table_name,
                             const sqlast::CreateTable &table_schema) {
  std::unique_lock<std::shared_mutex> lock(this->mtx1_);
  this->schema_.insert({table_name, table_schema});
}

// writes to kinds_ (w1), kind_to_tables_ (w1), kind_to_names_ (w1), shards_
// (w1)
void SharderState::AddShardKind(const ShardKind &kind, const ColumnName &pk) {
  std::unique_lock<std::shared_mutex> lock(this->mtx2_);
  this->kinds_.insert({kind, pk});
  // std::unique_lock<std::shared_mutex> unlock(this->mtx2_);
  this->kind_to_tables_.insert({kind, {}});
  this->kind_to_names_.insert({kind, {}});
  this->shards_.insert({kind, {}});
}

// writes to sharded_schema (w1)
void SharderState::AddUnshardedTable(const UnshardedTableName &table,
                                     const sqlast::CreateTable &create) {
  std::unique_lock<std::shared_mutex> lock(this->mtx3_);
  this->sharded_schema_.insert({table, create});
}

// writes to kind_to_tables_ (w1), kind_to_names_ (w1), sharded_by_ (w1),
// sharded_schema_ (w2)
void SharderState::AddShardedTable(
    const UnshardedTableName &table,
    const ShardingInformation &sharding_information,
    const sqlast::CreateTable &sharded_create_statement) {
  std::unique_lock<std::shared_mutex> lock(this->mtx2_);

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

// writes to shards_ (w2), create_index_ (w1)
std::list<const sqlast::AbstractStatement *> SharderState::CreateShard(
    const ShardKind &shard_kind, const UserId &user) {
  std::unique_lock<std::shared_mutex> lock(this->mtx4_);

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

// writes to shards_ (w3)
void SharderState::RemoveUserFromShard(const ShardKind &kind,
                                       const UserId &user_id) {
  std::unique_lock<std::shared_mutex> lock(this->mtx4_);
  this->shards_.at(kind).erase(user_id);
}

// Schema lookups.
// reads from schema_ (r1)
const sqlast::CreateTable &SharderState::GetSchema(
    const UnshardedTableName &table_name) const {
  std::shared_lock<std::shared_mutex> lock(this->mtx1_);
  return this->schema_.at(table_name);
}

// reads from sharded_schema_ (r1), sharded_by_ (r1)
bool SharderState::Exists(const UnshardedTableName &table) const {
  std::shared_lock<std::shared_mutex> lock(this->mtx2_);
  return this->sharded_schema_.count(table) > 0 ||
         this->sharded_by_.count(table) > 0;
}

// reads from sharded_by_ (r2)
bool SharderState::IsSharded(const UnshardedTableName &table) const {
  std::shared_lock<std::shared_mutex> lock(this->mtx2_);
  return this->sharded_by_.count(table) == 1;
}

// reads from sharded_by_ (r3)
const std::list<ShardingInformation> &SharderState::GetShardingInformation(
    const UnshardedTableName &table) const {
  std::shared_lock<std::shared_mutex> lock(this->mtx2_);
  return this->sharded_by_.at(table);
}

// reads from kinds_ (r1)
bool SharderState::IsPII(const UnshardedTableName &table) const {
  std::shared_lock<std::shared_mutex> lock(this->mtx2_);
  return this->kinds_.count(table) > 0;
}

// reads from kinds_ (r2)
const ColumnName &SharderState::PkOfPII(const UnshardedTableName &table) const {
  std::shared_lock<std::shared_mutex> lock(this->mtx2_);
  return this->kinds_.at(table);
}

// reads from shards_ (r1)
bool SharderState::ShardExists(const ShardKind &shard_kind,
                               const UserId &user) const {
  std::shared_lock<std::shared_mutex> lock(this->mtx2_);
  return this->shards_.at(shard_kind).count(user) > 0;
}

// reads from shards_ (r2)
const std::unordered_set<UserId> &SharderState::UsersOfShard(
    const ShardKind &kind) const {
  std::shared_lock<std::shared_mutex> lock(this->mtx2_);
  return this->shards_.at(kind);
}

// reads from kind_to_names_ (r1)
const std::unordered_set<UnshardedTableName> &SharderState::TablesInShard(
    const ShardKind &kind) const {
  std::shared_lock<std::shared_mutex> lock(this->mtx2_);
  return this->kind_to_names_.at(kind);
}

// Manage secondary indices.
// reads from index_to_flow_ (r1)
bool SharderState::HasIndexFor(const UnshardedTableName &table_name,
                               const ColumnName &column_name,
                               const ColumnName &shard_by) const {
  std::shared_lock<std::shared_mutex> lock(this->mtx5_);
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

// ? reads or writes to indices_ (w1 or r1). Not const, but seems like a reader
// ! TODO: Change to const
const std::unordered_set<ColumnName> &SharderState::IndicesFor(
    const UnshardedTableName &table_name) {
  std::unique_lock<std::shared_mutex> lock(this->mtx4_);
  // if table_name is in indices .count == 1
  return this->indices_.at(table_name);
  // else
  // return {};
  // return this->indices_[table_name];
}

// reads from index_to_flow_ (r2)
const FlowName &SharderState::IndexFlow(const UnshardedTableName &table_name,
                                        const ColumnName &column_name,
                                        const ColumnName &shard_by) const {
  std::shared_lock<std::shared_mutex> lock(this->mtx5_);
  return this->index_to_flow_.at(table_name).at(column_name).at(shard_by);
}

// writes to indices_ (w1 or w2), index_to_flow_ (w/r3?), create_index_ (w2)
void SharderState::CreateIndex(const ShardKind &shard_kind,
                               const UnshardedTableName &table_name,
                               const ColumnName &column_name,
                               const ColumnName &shard_by,
                               const FlowName &flow_name,
                               const sqlast::CreateIndex &create_index_stmt,
                               bool unique) {
  std::unique_lock<std::shared_mutex> lock(this->mtx4_);
  this->indices_[table_name].insert(column_name);
  this->index_to_flow_[table_name][column_name][shard_by] = flow_name;
  if (!unique) {
    this->create_index_[shard_kind].push_back(create_index_stmt);
  }
}

}  // namespace shards
}  // namespace pelton
