// Defines the state of our adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#ifndef SHARDS_STATE_H_
#define SHARDS_STATE_H_

#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

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

// The name of the table representing users that own shards of this kind.
using ShardKind = std::string;

// The name of a table from the original unsharded schema.
using TableName = std::string;

// A unique identifier that represents a user.
// This is used to come up with shard names for new users, and to track
// which shard belongs to which user.
using UserId = std::string;

using ColumnName = std::string;

// Valid SQL CreateTable statement formatted and ready to use to create
// some table.
using CreateStatement = std::string;

class SharderState {
 public:
  // Constructor.
  SharderState() {}

  // Not copyable or movable.
  SharderState(const SharderState &) = delete;
  SharderState &operator=(const SharderState &) = delete;
  SharderState(const SharderState &&) = delete;
  SharderState &operator=(const SharderState &&) = delete;

  // Destructor.
  ~SharderState() {}

  // Initialization.
  void Initialize(const std::string &dir_path);

  // Schema manipulations.
  void AddShardKind(const ShardKind &kind);

  void AddUnshardedTable(const TableName &table,
                         const CreateStatement &create_statement);

  void AddShardedTable(const TableName &table, const ShardKind &kind,
                       const CreateStatement &create_statement);

  void AddTablePrimaryKeys(const TableName &table,
                           std::unordered_set<ColumnName> &&keys);

  // Schema lookups.
  bool Exists(const TableName &table) const;

  bool ExistsInShardKind(const ShardKind &shard, const TableName &table) const;

  std::optional<ShardKind> ShardKindOf(const TableName &table) const;

  bool IsPII(const TableName &table) const;

  const std::unordered_set<ColumnName> &TableKeys(const TableName &table) const;

 private:
  // Directory in which all shards are stored.
  std::string dir_path_;

  // All shard kinds as determined from the schema.
  std::unordered_set<ShardKind> kinds_;

  // Maps a shard kind into the names of all contained tables.
  // Invariant: a table can at most belong to one shard kind.
  std::unordered_map<ShardKind, std::unordered_set<TableName>> kind_to_tables_;

  // Inverse mapping of kind_to_tables.
  // If a table is unmapped by this map, then it does not belong to any shard
  // kind, and data in it are not owned or related to any user.
  std::unordered_map<TableName, ShardKind> table_to_kind_;

  // Stores all the users identifiers which already have had shards
  // created for them.
  std::unordered_map<ShardKind, std::unordered_set<UserId>> shards_;

  // Maps every table in the overall schema to its create table statement.
  // This can be used to create that table in a new shard.
  std::unordered_map<TableName, CreateStatement> schema_;

  // Maps a table to the set of its primary keys.
  std::unordered_map<TableName, std::unordered_set<ColumnName>> primary_keys_;
};

}  // namespace shards

#endif  // SHARDS_STATE_H_
