// Defines the state of our adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#ifndef SHARDS_STATE_H_
#define SHARDS_STATE_H_

#include <list>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "shards/sqlconnections/pool.h"

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

using ColumnIndex = int;

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

  // Accessors.
  const std::string &dir_path() { return this->dir_path_; }

  // Initialization.
  void Initialize(const std::string &dir_path);

  // Schema manipulations.
  void AddShardKind(const ShardKind &kind);

  void AddUnshardedTable(const TableName &table,
                         const CreateStatement &create_statement);

  void AddShardedTable(const TableName &table, const ShardKind &kind,
                       const std::pair<ColumnName, ColumnIndex> &shard_by,
                       const CreateStatement &create_statement);

  std::list<CreateStatement> CreateShard(const ShardKind &shard_kind,
                                         const UserId &user);

  // Schema lookups.
  bool Exists(const TableName &table) const;

  std::optional<ShardKind> ShardKindOf(const TableName &table) const;

  std::optional<std::pair<ColumnName, ColumnIndex>> ShardedBy(
      const TableName &table) const;

  bool IsPII(const TableName &table) const;

  bool ShardExists(const ShardKind &shard_kind, const UserId &user) const;

  // Sqlite3 Connection pool interace.
  bool ExecuteStatement(const std::string &shard_suffix,
                        const std::string &sql_statement);

 private:
  // Directory in which all shards are stored.
  std::string dir_path_;

  // All shard kinds as determined from the schema.
  std::unordered_set<ShardKind> kinds_;

  // Maps a shard kind into the names of all contained tables.
  // Invariant: a table can at most belong to one shard kind.
  std::unordered_map<ShardKind, std::list<TableName>> kind_to_tables_;

  // Inverse mapping of kind_to_tables.
  // If a table is unmapped by this map, then it does not belong to any shard
  // kind, and data in it are not owned or related to any user.
  std::unordered_map<TableName, ShardKind> table_to_kind_;

  // Maps a table to the column name containing the data it is sharded by.
  // This column is a foreign key to a PII table or to another table in the
  // shard. This column is removed from the sharded schema of this table, and
  // only exists logically in the pre-sharded schema.
  // Column index is the index of the column in the pre-sharded table definition
  // and it is used to rewrite statements that rely on the order of columns
  // without specifying them explicitly.
  std::unordered_map<TableName, std::pair<ColumnName, ColumnIndex>> sharded_by_;

  // Stores all the users identifiers which already have had shards
  // created for them.
  std::unordered_map<ShardKind, std::unordered_set<UserId>> shards_;

  // Maps every table in the overall schema to its create table statement.
  // This can be used to create that table in a new shard.
  std::unordered_map<TableName, CreateStatement> schema_;

  // Connection pool that manages the underlying sqlite3 databases.
  sqlconnections::ConnectionPool pool_;
};

}  // namespace shards

#endif  // SHARDS_STATE_H_
