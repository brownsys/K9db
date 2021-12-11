// Defines the state of our sharding adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#ifndef PELTON_SHARDS_STATE_H_
#define PELTON_SHARDS_STATE_H_

#include <list>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "pelton/shards/types.h"
#include "pelton/shards/upgradable_lock.h"
#include "pelton/sql/lazy_executor.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {

// Our design splits a database into many shards, each shard belongs to a unique
// user and contains only that users data, and has the schema for tables related
// to that user only.
// In case we have different types of users (e.g. a student and instructor
// tables), we will end up having two kinds of shards: one for students and one
// for instructors. Each kind consists of many shards (as many as users of that
// kind).
// SharderState helps us track how many such categories exist, what
// schema they have, and how many shards (and for which user) exists for these
// kinds. It also keeps an in-memory cache of the user tables, and uses them
// as a shard directory.
class SharderState {
 public:
  // Constructor.
  SharderState() = default;

  // Not copyable or movable.
  SharderState(const SharderState &) = delete;
  SharderState &operator=(const SharderState &) = delete;
  SharderState(const SharderState &&) = delete;
  SharderState &operator=(const SharderState &&) = delete;

  // Schema manipulations.
  void AddSchema(const UnshardedTableName &table_name,
                 const sqlast::CreateTable &table_schema, int pk_index,
                 const std::string &pk);

  void AddShardKind(const ShardKind &kind, const ColumnName &pk);

  void AddUnshardedTable(const UnshardedTableName &table,
                         const sqlast::CreateTable &create);

  void AddShardedTable(const UnshardedTableName &table,
                       const ShardingInformation &sharding_information,
                       const sqlast::CreateTable &sharded_create_statement);

  void AddAccessorIndex(
      const ShardKind &kind, const UnshardedTableName &table,
      const ColumnName &accessor_column, const ColumnName &shard_by_column,
      const IndexName &index_name,
      const std::unordered_map<ColumnName, sqlast::ColumnDefinition::Type>
          &anonymize,
      const bool is_sharded);

  std::list<const sqlast::AbstractStatement *> CreateShard(
      const ShardKind &shard_kind, const UserId &user);

  void RemoveUserFromShard(const ShardKind &kind, const UserId &user_id);

  std::optional<sqlast::CreateTable> RemoveSchema(const UnshardedTableName &table_name);

  // Schema lookups.
  const sqlast::CreateTable &GetSchema(
      const UnshardedTableName &table_name) const;

  const std::pair<int, std::string> &GetPk(
      const UnshardedTableName &table_name) const;

  bool Exists(const UnshardedTableName &table) const;

  bool IsSharded(const UnshardedTableName &table) const;

  const std::list<ShardingInformation> &GetShardingInformation(
      const UnshardedTableName &table) const;

  std::vector<const ShardingInformation *> GetShardingInformationFor(
      const UnshardedTableName &table,
      const std::string &shard_kind) const;

  bool IsPII(const UnshardedTableName &table) const;

  const ColumnName &PkOfPII(const UnshardedTableName &table) const;

  bool ShardExists(const ShardKind &shard_kind, const UserId &user) const;

  const std::unordered_set<UserId> &UsersOfShard(const ShardKind &kind) const;

  const std::unordered_set<UnshardedTableName> &TablesInShard(
      const ShardKind &kind) const;

  // Manage secondary indices.
  bool HasIndexFor(const UnshardedTableName &table_name,
                   const ColumnName &column_name,
                   const ColumnName &shard_by) const;

  bool HasIndicesFor(const UnshardedTableName &table_name) const;

  const std::unordered_set<ColumnName> &IndicesFor(
      const UnshardedTableName &table_name) const;

  const FlowName &IndexFlow(const UnshardedTableName &table_name,
                            const ColumnName &column_name,
                            const ColumnName &shard_by) const;

  void CreateIndex(const ShardKind &shard_kind,
                   const UnshardedTableName &table_name,
                   const ColumnName &column_name, const ColumnName &shard_by,
                   const FlowName &flow_name,
                   const sqlast::CreateIndex &create_index_stmt, bool unique);

  inline std::string GenerateUniqueIndexName(UniqueLock *lock) {
      this->_unique_index_ctr += 1;
      return "_unq_indx_" + std::to_string(this->_unique_index_ctr);
  }

  sql::SqlResult NumShards() const;

  // Returns all accessor indices associated with a given shard
  bool HasAccessorIndices(const ShardKind &kind) const;
  const std::vector<AccessorIndexInformation> &GetAccessorIndices(
      const ShardKind &kind) const;
  std::vector<const AccessorIndexInformation *> GetAccessorInformationFor(
      const ShardKind &kind,
      const UnshardedTableName &table_name) const;

  size_t NumShards() {
    size_t count = 0;
    for (auto &s : shards_) {
      count += s.second.size();
    }
    return count;
  }

  // Synchronization.
  UniqueLock WriterLock();
  SharedLock ReaderLock() const;

 private:
  // The logical (unsharded) schema of every table.
  std::unordered_map<UnshardedTableName, sqlast::CreateTable> schema_;
  std::unordered_map<UnshardedTableName, std::pair<int, std::string>> pks_;

  // All shard kinds as determined from the schema.
  // Maps a shard kind (e.g. a table name with PII) to the primary key of that
  // table.
  std::unordered_map<ShardKind, ColumnName> kinds_;

  // Maps a shard kind into the names of all contained tables.
  // Invariant: a table can at most belong to one shard kind.
  std::unordered_map<ShardKind, std::list<ShardedTableName>> kind_to_tables_;
  std::unordered_map<ShardKind, std::unordered_set<UnshardedTableName>>
      kind_to_names_;

  // Maps a table to the its sharding information.
  // If a table is unmapped by this map, then it is not sharded.
  // A table mapped by this map is sharded (by one or more columns).
  // Each entry in the mapped list specifies one such shard, including
  // the name of the table when sharded that way, as well as the column it is
  // sharded by.
  // When a table is sharded in several ways, it means that multiple foreign
  // keys were specified as OWNER, and data inserted into that table is
  // duplicated along the shards.
  // The shard by column is removed from the actual sharded table as it can be
  // deduced from the shard.
  std::unordered_map<UnshardedTableName, std::list<ShardingInformation>>
      sharded_by_;

  // Stores all the users identifiers which already have had shards
  // created for them.
  std::unordered_map<ShardKind, std::unordered_set<UserId>> shards_;

  // Maps every (sharded) table in the overall schema to its sharded schema.
  // This can be used to create that table in a new shard.
  // The schema matches what is stored physically in the DB after
  // sharding and other transformations.
  std::unordered_map<ShardedTableName, sqlast::CreateTable> sharded_schema_;

  // Secondary indices.
  std::unordered_map<ShardKind, std::vector<sqlast::CreateIndex>> create_index_;

  // Map to store accessor indices
  std::unordered_map<ShardKind, std::vector<AccessorIndexInformation>>
      accessor_index_;

  // All columns in a table that have an index.
  std::unordered_map<UnshardedTableName, std::unordered_set<ColumnName>>
      indices_;

  // table-name -> column with an index name -> shard by column for which
  //    the index provide values -> the corresponding flow name of the index.
  std::unordered_map<
      UnshardedTableName,
      std::unordered_map<ColumnName, std::unordered_map<ColumnName, FlowName>>>
      index_to_flow_;

  // Our implementation of an upgradable shared mutex.
  mutable UpgradableMutex mtx_;

  unsigned int _unique_index_ctr = 0;
};

}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_STATE_H_
