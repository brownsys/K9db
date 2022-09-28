// Defines the state of our sharding adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#include "pelton/shards/state.h"

namespace pelton {
namespace shards {

/*
 * Table creation.
 */
void SharderState::AddTable(Table &&table) {
  for (const auto &descriptor : table.owners) {
    Shard &shard = this->shards_.at(descriptor->shard_kind);
    shard.owned_tables.insert(table.table_name);
    if (descriptor->type == InfoType::DIRECT) {
      if (descriptor->shard_kind != table.table_name) {
        this->dependencies_.at(descriptor->shard_kind) = true;
      }
    }
    if (descriptor->type == InfoType::TRANSITIVE) {
      auto &next_table = std::get<TransitiveInfo>(descriptor->info).next_table;
      this->dependencies_.at(next_table) = true;
    }
  }
  for (const auto &descriptor : table.accessors) {
    Shard &shard = this->shards_.at(descriptor->shard_kind);
    shard.accessor_tables.insert(table.table_name);
    if (descriptor->type == InfoType::DIRECT) {
      if (descriptor->shard_kind != table.table_name) {
        this->dependencies_.at(descriptor->shard_kind) = true;
      }
    }
    if (descriptor->type == InfoType::TRANSITIVE) {
      auto &next_table = std::get<TransitiveInfo>(descriptor->info).next_table;
      this->dependencies_.at(next_table) = true;
    }
  }
  this->dependencies_.emplace(table.table_name, false);
  this->tables_.emplace(table.table_name, std::move(table));
}

/*
 * Only for use when handling OWNS/ACCESSES.
 */
absl::Status SharderState::AddTableOwner(
    const TableName &table_name,
    std::vector<std::unique_ptr<ShardDescriptor>> &&owner) {
  if (this->dependencies_.at(table_name)) {
    return absl::InvalidArgumentError("OWNS into table with dependencies");
  }
  for (std::unique_ptr<ShardDescriptor> &desc : owner) {
    Shard &shard = this->shards_.at(desc->shard_kind);
    shard.owned_tables.insert(table_name);
    Table &tbl = this->tables_.at(table_name);
    tbl.owners.push_back(std::move(desc));
  }
  return absl::OkStatus();
}
absl::Status SharderState::AddTableAccessor(
    const TableName &table_name,
    std::vector<std::unique_ptr<ShardDescriptor>> &&access) {
  if (this->dependencies_.at(table_name)) {
    return absl::InvalidArgumentError("ACCESSES into table with dependencies");
  }
  for (std::unique_ptr<ShardDescriptor> &desc : access) {
    Shard &shard = this->shards_.at(desc->shard_kind);
    shard.accessor_tables.insert(table_name);
    Table &tbl = this->tables_.at(table_name);
    tbl.accessors.push_back(std::move(desc));
  }
  return absl::OkStatus();
}

/*
 * Table lookup.
 */
bool SharderState::TableExists(const TableName &table_name) const {
  return this->tables_.count(table_name) == 1;
}
bool SharderState::IsOwned(const TableName &table_name) const {
  return this->tables_.at(table_name).owners.size() > 0;
}
bool SharderState::IsAccessed(const TableName &table_name) const {
  const Table &table = this->tables_.at(table_name);
  return table.owners.size() > 0 || table.accessors.size() > 0;
}
Table &SharderState::GetTable(const TableName &table_name) {
  return this->tables_.at(table_name);
}
const Table &SharderState::GetTable(const TableName &table_name) const {
  return this->tables_.at(table_name);
}

/*
 * Shard / shardkind lookups.
 */
void SharderState::AddShardKind(const ShardKind &shard_kind,
                                const ColumnName &id_column,
                                ColumnIndex id_column_index) {
  Shard &shard = this->shards_[shard_kind];
  shard.shard_kind = shard_kind;
  shard.id_column = id_column;
  shard.id_column_index = id_column_index;
  this->users_.try_emplace(shard_kind);
}
bool SharderState::ShardKindExists(const ShardKind &shard_kind) const {
  return this->shards_.count(shard_kind) == 1;
}
Shard &SharderState::GetShard(const ShardKind &shard_kind) {
  return this->shards_.at(shard_kind);
}
const Shard &SharderState::GetShard(const ShardKind &shard_kind) const {
  return this->shards_.at(shard_kind);
}

/*
 * Track users.
 */
void SharderState::IncrementUsers(const ShardKind &kind) {
  this->users_.at(kind)++;
}
void SharderState::DecrementUsers(const ShardKind &kind) {
  this->users_.at(kind)--;
}

/*
 * Debugging information / statistics.
 */
std::vector<std::pair<ShardKind, uint64_t>> SharderState::NumShards() const {
  std::vector<std::pair<ShardKind, uint64_t>> result;
  for (const auto &[kind, count] : this->users_) {
    result.emplace_back(kind, count);
  }
  return result;
}

}  // namespace shards
}  // namespace pelton
