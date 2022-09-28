// Defines the state of our sharding adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#ifndef PELTON_SHARDS_STATE_H_
#define PELTON_SHARDS_STATE_H_

#include <atomic>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/status/status.h"
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

  // Table creation.
  void AddTable(Table &&table);

  // Only for use when handling OWNS/ACCESSES.
  absl::Status AddTableOwner(
      const TableName &table_name,
      std::vector<std::unique_ptr<ShardDescriptor>> &&owner);

  absl::Status AddTableAccessor(
      const TableName &table_name,
      std::vector<std::unique_ptr<ShardDescriptor>> &&access);

  // Table lookup.
  bool TableExists(const TableName &table_name) const;
  bool IsOwned(const TableName &table_name) const;
  bool IsAccessed(const TableName &table_name) const;
  Table &GetTable(const TableName &table_name);
  const Table &GetTable(const TableName &table_name) const;

  // Shard / shardkind lookups.
  void AddShardKind(const ShardKind &shard_kind, const ColumnName &id_column,
                    ColumnIndex id_column_index);
  bool ShardKindExists(const ShardKind &shard_kind) const;
  Shard &GetShard(const ShardKind &shard_kind);
  const Shard &GetShard(const ShardKind &shard_kind) const;

  // Track users.
  void IncrementUsers(const ShardKind &kind);
  void DecrementUsers(const ShardKind &kind);

  // Debugging information / statistics.
  std::vector<std::pair<ShardKind, uint64_t>> NumShards() const;

 private:
  // All the different shard kinds that currently exist in the system.
  // Does not contain any information about instances of these shards, instead
  // merely contains schematic information about included tables and their
  // ownership and accessorship.
  std::unordered_map<ShardKind, Shard> shards_;

  // All the tables defined by the schema.
  std::unordered_map<TableName, Table> tables_;

  // Mark whether we encountered tables that depend on each given table or not.
  // Checking this, we can guarantee that all OWNS into a table are created
  // before any OWNED BY / ACCESSED BY annotations refer to it.
  std::unordered_map<TableName, bool> dependencies_;

  // Counts of users currently in the system.
  std::unordered_map<ShardKind, std::atomic<uint64_t>> users_;
};

}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_STATE_H_
