// Defines the state of our sharding adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#include "k9db/shards/state.h"

#include "glog/logging.h"

namespace k9db {
namespace shards {

/*
 * Table creation.
 */
Table *SharderState::AddTable(Table &&table) {
  for (const auto &descriptor : table.owners) {
    Shard &shard = this->shards_.at(descriptor->shard_kind);
    shard.owned_tables.insert(table.table_name);
    Table *parent = nullptr;
    switch (descriptor->type) {
      case InfoType::DIRECT: {
        if (descriptor->shard_kind != table.table_name) {
          parent = &this->tables_.at(descriptor->shard_kind);
        }
        break;
      }
      case InfoType::TRANSITIVE: {
        const TransitiveInfo &info = std::get<TransitiveInfo>(descriptor->info);
        parent = &this->tables_.at(info.next_table);
        break;
      }
      case InfoType::VARIABLE: {
        const VariableInfo &info = std::get<VariableInfo>(descriptor->info);
        parent = &this->tables_.at(info.origin_relation);
        break;
      }
    }
    if (parent != nullptr) {
      parent->dependents.emplace_back(table.table_name, descriptor.get());
    }
  }
  for (const auto &descriptor : table.accessors) {
    Shard &shard = this->shards_.at(descriptor->shard_kind);
    shard.accessor_tables.insert(table.table_name);
    Table *parent = nullptr;
    switch (descriptor->type) {
      case InfoType::DIRECT: {
        parent = &this->tables_.at(descriptor->shard_kind);
        break;
      }
      case InfoType::TRANSITIVE: {
        const TransitiveInfo &info = std::get<TransitiveInfo>(descriptor->info);
        parent = &this->tables_.at(info.next_table);
        break;
      }
      case InfoType::VARIABLE: {
        const VariableInfo &info = std::get<VariableInfo>(descriptor->info);
        parent = &this->tables_.at(info.origin_relation);
        break;
      }
    }
    parent->access_dependents.emplace_back(table.table_name, descriptor.get());
  }
  auto pair = this->tables_.emplace(table.table_name, std::move(table));
  return &pair.first->second;
}

/*
 * Only for use when handling OWNS/ACCESSES.
 */
absl::Status SharderState::AddTableOwner(
    const TableName &table_name,
    std::vector<std::unique_ptr<ShardDescriptor>> &&owner) {
  Table &tbl = this->tables_.at(table_name);
  if (tbl.dependents.size() > 0 || tbl.access_dependents.size() > 0) {
    return absl::InvalidArgumentError("OWNS into table with dependencies");
  }
  for (std::unique_ptr<ShardDescriptor> &desc : owner) {
    if (desc->type != InfoType::VARIABLE) {
      return absl::InvalidArgumentError("AddTableOwner bad annotation");
    }
    // Look up origin table.
    const VariableInfo &info = std::get<VariableInfo>(desc->info);
    Table &origin = this->tables_.at(info.origin_relation);
    // Update shard and this table.
    Shard &shard = this->shards_.at(desc->shard_kind);
    shard.owned_tables.insert(table_name);
    tbl.owners.push_back(std::move(desc));
    // Update origin table.
    ShardDescriptor *ptr = tbl.owners.back().get();
    origin.dependents.emplace_back(table_name, ptr);
  }
  return absl::OkStatus();
}
absl::Status SharderState::AddTableAccessor(
    const TableName &table_name,
    std::vector<std::unique_ptr<ShardDescriptor>> &&access) {
  Table &tbl = this->tables_.at(table_name);
  if (tbl.dependents.size() > 0 || tbl.access_dependents.size() > 0) {
    return absl::InvalidArgumentError("ACCESSES into table with dependencies");
  }
  for (std::unique_ptr<ShardDescriptor> &desc : access) {
    if (desc->type != InfoType::VARIABLE) {
      return absl::InvalidArgumentError("AddTableAccessor bad annotation");
    }
    // Look up origin table.
    const VariableInfo &info = std::get<VariableInfo>(desc->info);
    Table &origin = this->tables_.at(info.origin_relation);
    // Update shard and this table.
    Shard &shard = this->shards_.at(desc->shard_kind);
    shard.accessor_tables.insert(table_name);
    tbl.accessors.push_back(std::move(desc));
    // Update origin table.
    ShardDescriptor *ptr = tbl.accessors.back().get();
    origin.access_dependents.emplace_back(table_name, ptr);
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
std::vector<std::string> SharderState::GetTables() const {
  std::vector<std::string> vec;
  for (const auto &[table_name, _] : this->tables_) {
    vec.push_back(table_name);
  }
  return vec;
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
void SharderState::IncrementUsers(const ShardKind &kind, size_t count) {
  this->users_.at(kind) += count;
}
void SharderState::DecrementUsers(const ShardKind &kind, size_t count) {
  this->users_.at(kind) -= count;
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
}  // namespace k9db
