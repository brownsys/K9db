// Defines the state of our sharding adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#ifndef PELTON_SHARDS_STATE_H_
#define PELTON_SHARDS_STATE_H_

#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "pelton/shards/pool.h"
#include "pelton/shards/types.h"
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

  // Accessors.
  ConnectionPool &connection_pool() { return this->connection_pool_; }

  // Initialization.
  void Initialize(const std::string &db_username,
                  const std::string &db_password);

  // Schema manipulations.
  void AddSchema(const UnshardedTableName &table_name,
                 const sqlast::CreateTable &table_schema);

  void AddShardKind(const ShardKind &kind, const ColumnName &pk);

  void AddUnshardedTable(const UnshardedTableName &table,
                         const sqlast::CreateTable &create);

  void AddShardedTable(const UnshardedTableName &table,
                       const ShardingInformation &sharding_information,
                       const sqlast::CreateTable &sharded_create_statement);

  std::list<const sqlast::AbstractStatement *> CreateShard(
      const ShardKind &shard_kind, const UserId &user);

  void RemoveUserFromShard(const ShardKind &kind, const UserId &user_id);

  // Schema lookups.
  const sqlast::CreateTable &GetSchema(
      const UnshardedTableName &table_name) const;

  bool Exists(const UnshardedTableName &table) const;

  bool IsSharded(const UnshardedTableName &table) const;

  const std::list<ShardingInformation> &GetShardingInformation(
      const UnshardedTableName &table) const;

  bool IsPII(const UnshardedTableName &table) const;

  const ColumnName &PkOfPII(const UnshardedTableName &table) const;

  bool ShardExists(const ShardKind &shard_kind, const UserId &user) const;

  const std::unordered_set<UserId> &UsersOfShard(const ShardKind &kind) const;

  // Manage secondary indices.
  bool HasIndexFor(const UnshardedTableName &table_name,
                   const ColumnName &column_name,
                   const ColumnName &shard_by) const;

  const std::unordered_set<ColumnName> &IndicesFor(
      const UnshardedTableName &table_name);

  const FlowName &IndexFlow(const UnshardedTableName &table_name,
                            const ColumnName &column_name,
                            const ColumnName &shard_by) const;

  void CreateIndex(const ShardKind &shard_kind,
                   const UnshardedTableName &table_name,
                   const ColumnName &column_name, const ColumnName &shard_by,
                   const FlowName &flow_name,
                   const sqlast::CreateIndex &create_index_stmt, bool unique);

  // Save state to durable file.
  void Save(const std::string &dir_path);

  // Load state from its durable file (if exists).
  void Load(const std::string &dir_path);

  size_t NumShards() {
    size_t count = 0;
    for (auto &s : shards_) {
      count += s.second.size();
    }
    return count;
  }

 private:
  // The logical (unsharded) schema of every table.
  std::unordered_map<UnshardedTableName, sqlast::CreateTable> schema_;

  // All shard kinds as determined from the schema.
  // Maps a shard kind (e.g. a table name with PII) to the primary key of that
  // table.
  std::unordered_map<ShardKind, ColumnName> kinds_;

  // Maps a shard kind into the names of all contained tables.
  // Invariant: a table can at most belong to one shard kind.
  std::unordered_map<ShardKind, std::list<ShardedTableName>> kind_to_tables_;

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

  // Connection pool that manages the underlying sqlite3 databases.
  ConnectionPool connection_pool_;

  // Secondary indices.
  std::unordered_map<ShardKind, std::vector<sqlast::CreateIndex>> create_index_;

  // All columns in a table that have an index.
  std::unordered_map<UnshardedTableName, std::unordered_set<ColumnName>>
      indices_;

  // table-name -> column with an index name -> shard by column for which
  //    the index provide values -> the corresponding flow name of the index.
  std::unordered_map<
      UnshardedTableName,
      std::unordered_map<ColumnName, std::unordered_map<ColumnName, FlowName>>>
      index_to_flow_;
};

}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_STATE_H_
