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

// Schema manipulations.
void SharderState::AddSchema(const UnshardedTableName &table_name,
                             const sqlast::CreateTable &table_schema) {
  // writer lock for schema_
  std::unique_lock<std::shared_mutex> lock(this->mtx1_);
  this->schema_.insert({table_name, table_schema});
}

void SharderState::AddShardKind(const ShardKind &kind, const ColumnName &pk) {
  // writer lock for kinds_, kind_to_tables, kind_to_names
  std::unique_lock<std::shared_mutex> lock(this->mtx2_);
  // writer lock for shards_
  std::unique_lock<std::shared_mutex> lock2(this->mtx3_);
  this->kinds_.insert({kind, pk});
  this->kind_to_tables_.insert({kind, {}});
  this->kind_to_names_.insert({kind, {}});
  this->shards_.insert({kind, {}});
}

void SharderState::AddUnshardedTable(const UnshardedTableName &table,
                                     const sqlast::CreateTable &create) {
  // writer lock for sharded_schema_
  std::unique_lock<std::shared_mutex> lock(this->mtx3_);
  this->sharded_schema_.insert({table, create});
}

void SharderState::AddShardedTable(
    const UnshardedTableName &table,
    const ShardingInformation &sharding_information,
    const sqlast::CreateTable &sharded_create_statement) {
  // writer lock for kind_to_tables_, kind_to_names_
  std::unique_lock<std::shared_mutex> lock1(this->mtx2_);
  // writer lock for sharded_by_, sharded_schema_
  std::unique_lock<std::shared_mutex> lock2(this->mtx3_);

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
  // reader lock for kind_to_tables_
  std::shared_lock<std::shared_mutex> lock(this->mtx2_);
  // writer lock for shards_ and sharded_schema_
  std::unique_lock<std::shared_mutex> lock2(this->mtx3_);
  // reader lock for create_index_
  std::shared_lock<std::shared_mutex> lock4(this->mtx4_);

  // Mark shard for this user as created!
  this->shards_.at(shard_kind).insert(user);
  // Return the create table statements.
  std::list<const sqlast::AbstractStatement *> result;
  for (const ShardedTableName &table : this->kind_to_tables_.at(shard_kind)) {
    result.push_back(&this->sharded_schema_.at(table));
  }
  // if shard_kind exists as key in create_index_, add to result
  if (this->create_index_.find(shard_kind) != this->create_index_.end()) {
    for (const sqlast::CreateIndex &create_index :
         this->create_index_[shard_kind]) {
      result.push_back(&create_index);
    }
  }
  return result;
}

void SharderState::RemoveUserFromShard(const ShardKind &kind,
                                       const UserId &user_id) {
  // writer lock for shards_
  std::unique_lock<std::shared_mutex> lock(this->mtx3_);
  this->shards_.at(kind).erase(user_id);
}

// Schema lookups.
const sqlast::CreateTable &SharderState::GetSchema(
    const UnshardedTableName &table_name) const {
  // reader lock for schema_
  std::shared_lock<std::shared_mutex> lock(this->mtx1_);
  return this->schema_.at(table_name);
}

bool SharderState::Exists(const UnshardedTableName &table) const {
  // reader lock for sharded_schema_, sharded_by_
  std::shared_lock<std::shared_mutex> lock(this->mtx3_);
  return this->sharded_schema_.count(table) > 0 ||
         this->sharded_by_.count(table) > 0;
}

bool SharderState::IsSharded(const UnshardedTableName &table) const {
  // reader lock for sharded_by_
  std::shared_lock<std::shared_mutex> lock(this->mtx3_);
  return this->sharded_by_.count(table) == 1;
}

// reads from sharded_by_ (r3)
const std::list<ShardingInformation> &SharderState::GetShardingInformation(
    const UnshardedTableName &table) const {
  // reader lock for sharded_by_
  std::shared_lock<std::shared_mutex> lock(this->mtx3_);
  return this->sharded_by_.at(table);
}

bool SharderState::IsPII(const UnshardedTableName &table) const {
  // reader lock for kinds_
  std::shared_lock<std::shared_mutex> lock(this->mtx2_);
  return this->kinds_.count(table) > 0;
}

const ColumnName &SharderState::PkOfPII(const UnshardedTableName &table) const {
  // reader lock for kinds_
  std::shared_lock<std::shared_mutex> lock(this->mtx2_);
  return this->kinds_.at(table);
}

bool SharderState::ShardExists(const ShardKind &shard_kind,
                               const UserId &user) const {
  // reader lock for shards_
  std::shared_lock<std::shared_mutex> lock(this->mtx3_);
  return this->shards_.at(shard_kind).count(user) > 0;
}

const std::unordered_set<UserId> &SharderState::UsersOfShard(
    const ShardKind &kind) const {
  // reader lock for shards_
  std::shared_lock<std::shared_mutex> lock(this->mtx3_);
  return this->shards_.at(kind);
}

const std::unordered_set<UnshardedTableName> &SharderState::TablesInShard(
    const ShardKind &kind) const {
  // reader lock for kind_to_names_
  std::shared_lock<std::shared_mutex> lock(this->mtx2_);
  return this->kind_to_names_.at(kind);
}

// Manage secondary indices.
bool SharderState::HasIndexFor(const UnshardedTableName &table_name,
                               const ColumnName &column_name,
                               const ColumnName &shard_by) const {
  // reader lock for index_to_flow_
  std::shared_lock<std::shared_mutex> lock(this->mtx4_);
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
    const UnshardedTableName &table_name)  {
  // reader lock for indices_
  // std::shared_lock<std::shared_mutex> lock(this->mtx4_);
  // writer lock for indices_
  std::unique_lock<std::shared_mutex> lock(this->mtx4_);
  return this->indices_[table_name];
  // if table_name does not exist, return empty unordered_set
  // if (this->indices_.find(table_name) == this->indices_.end()) {
  //   const std::unordered_set<ColumnName> *empty_column =
  //       new std::unordered_set<ColumnName>{};
  //   return *empty_column;
  // } else {
  //   return this->indices_.at(table_name);
  // }
}

const FlowName &SharderState::IndexFlow(const UnshardedTableName &table_name,
                                        const ColumnName &column_name,
                                        const ColumnName &shard_by) const {
  // reader lock for index_to_flow_
  std::shared_lock<std::shared_mutex> lock(this->mtx4_);
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
  // writer lock for indices_, index_to_flow_, create_index_
  std::unique_lock<std::shared_mutex> lock(this->mtx4_);
  this->indices_[table_name].insert(column_name);
  this->index_to_flow_[table_name][column_name][shard_by] = flow_name;
  if (!unique) {
    this->create_index_[shard_kind].push_back(create_index_stmt);
  }
}

}  // namespace shards
}  // namespace pelton
