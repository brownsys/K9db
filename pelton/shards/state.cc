// Defines the state of our sharding adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#include "pelton/shards/state.h"

#include <stdlib.h>
#include <string.h>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"

namespace pelton {
namespace shards {

// Schema manipulations.
void SharderState::AddSchema(const UnshardedTableName &table_name,
                             const sqlast::CreateTable &table_schema,
                             int pk_index, const std::string &pk_name) {
  this->schema_.insert({table_name, table_schema});
  this->pks_.emplace(table_name,
                     std::pair<int, std::string>(pk_index, pk_name));
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

// Used to add an AccessorIndexInformation struct to sharder state
void SharderState::AddAccessorIndex(
    const ShardKind &kind, const UnshardedTableName &table,
    const ColumnName &accessor_column, const ColumnName &shard_by_column,
    const IndexName &index_name,
    const std::unordered_map<ColumnName, sqlast::ColumnDefinition::Type>
        &anonymize,
    const bool is_sharded) {
  // Create an AccessorIndexInformation
  AccessorIndexInformation accessor_information{
      kind,       table,     accessor_column, shard_by_column,
      index_name, anonymize, is_sharded};
  this->accessor_index_[kind].push_back(accessor_information);
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
  // if shard_kind exists as key in create_index_, add to result
  const auto &indices = this->create_index_[shard_kind];
  for (const sqlast::CreateIndex &create_index : indices) {
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

const std::pair<int, std::string> &SharderState::GetPk(
    const UnshardedTableName &table_name) const {
  return this->pks_.at(table_name);
}

bool SharderState::Exists(const UnshardedTableName &table) const {
  return this->sharded_schema_.count(table) > 0 ||
         this->sharded_by_.count(table) > 0;
}

bool SharderState::IsSharded(const UnshardedTableName &table) const {
  return this->sharded_by_.count(table) == 1;
}

// reads from sharded_by_ (r3)
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

bool SharderState::HasAccessorIndices(const ShardKind &kind) const {
  return this->accessor_index_.count(kind) == 1;
}
const std::vector<AccessorIndexInformation> &SharderState::GetAccessorIndices(
    const ShardKind &kind) const {
  return this->accessor_index_.at(kind);
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

bool SharderState::HasIndicesFor(const UnshardedTableName &table_name) const {
  return this->indices_.count(table_name) > 0;
}

const std::unordered_set<ColumnName> &SharderState::IndicesFor(
    const UnshardedTableName &table_name) const {
  return this->indices_.at(table_name);
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

sql::SqlResult SharderState::NumShards() const {
  std::vector<dataflow::Record> records;
  for (const auto &[kind, set] : this->shards_) {
    records.emplace_back(dataflow::SchemaFactory::NUM_SHARDS_SCHEMA, true,
                         std::make_unique<std::string>(kind),
                         static_cast<uint64_t>(set.size()));
  }
  return sql::SqlResult(sql::SqlResultSet(
      dataflow::SchemaFactory::NUM_SHARDS_SCHEMA, {std::move(records), {}}));
}

// Synchronization.
UniqueLock SharderState::WriterLock() { return UniqueLock(&this->mtx_); }
SharedLock SharderState::ReaderLock() const { return SharedLock(&this->mtx_); }

}  // namespace shards
}  // namespace pelton
