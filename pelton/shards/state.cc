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
void SharderState::Initialize(const std::string &dir_path) {
  this->dir_path_ = dir_path;
  if (dir_path.back() != '/') {
    this->dir_path_ += "/";
  }
  this->connection_pool_.Initialize(this->dir_path_);
}

// Schema manipulations.
void SharderState::AddShardKind(const ShardKind &kind, const ColumnName &pk) {
  this->kinds_.insert({kind, pk});
  this->kind_to_tables_.insert({kind, {}});
  this->shards_.insert({kind, {}});
}

void SharderState::AddUnshardedTable(const UnshardedTableName &table,
                                     const CreateStatement &create_str) {
  this->sharded_schema_.insert({table, create_str});
}

void SharderState::AddShardedTable(
    const UnshardedTableName &table,
    const ShardingInformation &sharding_information,
    const CreateStatement &sharded_create_statement) {
  // Record that the shard kind contains this sharded table.
  this->kind_to_tables_.at(sharding_information.shard_kind)
      .push_back(sharding_information.sharded_table_name);
  // Map the unsharded name to its sharding information.
  this->sharded_by_[table].push_back(sharding_information);
  // Store the sharded schema.
  this->sharded_schema_.insert(
      {sharding_information.sharded_table_name, sharded_create_statement});
}

std::list<CreateStatement> SharderState::CreateShard(
    const ShardKind &shard_kind, const UserId &user) {
  // Mark shard for this user as created!
  this->shards_.at(shard_kind).insert(user);
  // Return the create table statements.
  std::list<CreateStatement> result;
  for (const ShardedTableName &table : this->kind_to_tables_.at(shard_kind)) {
    result.push_back(this->sharded_schema_.at(table));
  }
  return result;
}

void SharderState::RemoveUserFromShard(const ShardKind &kind,
                                       const UserId &user_id) {
  this->shards_.at(kind).erase(user_id);
}

// Schema lookups.
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

// Raw records.
void SharderState::AddRawRecord(const std::string &table_name,
                                const std::vector<std::string> values,
                                const std::vector<std::string> columns,
                                bool positive) {
  this->raw_records_.emplace_back(table_name, values, columns, positive);
}

const std::vector<RawRecord> &SharderState::GetRawRecords() const {
  return this->raw_records_;
}

void SharderState::ClearRawRecords() { this->raw_records_.clear(); }

}  // namespace shards
}  // namespace pelton
