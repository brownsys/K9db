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
#include <unordered_set>
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
  Table *AddTable(Table &&table);

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
  std::vector<std::string> GetTables() const;

  // Shard / shardkind lookups.
  void AddShardKind(const ShardKind &shard_kind, const ColumnName &id_column,
                    ColumnIndex id_column_index);
  bool ShardKindExists(const ShardKind &shard_kind) const;
  Shard &GetShard(const ShardKind &shard_kind);
  const Shard &GetShard(const ShardKind &shard_kind) const;

  // Track users.
  void IncrementUsers(const ShardKind &kind, size_t count);
  void DecrementUsers(const ShardKind &kind, size_t count);

  // To create unique index names.
  uint64_t IncrementIndexCount() { return this->index_count_++; }

  // Debugging information / statistics.
  std::vector<std::pair<ShardKind, uint64_t>> NumShards() const;
  std::list<std::pair<TableName, const Table *>> ReverseTables() const {
    std::list<std::pair<TableName, const Table *>> vec;
    for (const auto &[table_name, table] : this->tables_) {
      vec.emplace_front(table_name, &table);
    }
    return vec;
  }

  // Combinators for recursing over the transitive closure of owners and
  // dependents.
  // DFS starting from table and going up or down.
  template <typename A>
  void VisitOwnersUp(const TableName &table, Visitor<A> *v, A arg) const {
    v->Initialize(&this->tables_);
    v->template VisitOwners<true>(table, arg);
  }
  template <typename A>
  void VisitOwnersDown(const TableName &table, Visitor<A> *v, A arg) const {
    v->Initialize(&this->tables_);
    v->template VisitOwners<false>(table, arg);
  }
  template <typename A>
  void VisitAccessorUp(const TableName &table, Visitor<A> *v, A arg) const {
    v->Initialize(&this->tables_);
    v->template VisitAccessors<true>(table, arg);
  }
  template <typename A>
  void VisitAccessorDown(const TableName &table, Visitor<A> *v, A arg) const {
    v->Initialize(&this->tables_);
    v->template VisitAccessors<false>(table, arg);
  }

  // Add locks for the given table (which was just created).
  void AddTableLocks(const Table &table);

  // Allows us to disable serializability during priming.
  // TODO(babman): implement this.
  void TurnOnSerializable() {}

 private:
  // All the different shard kinds that currently exist in the system.
  // Does not contain any information about instances of these shards, instead
  // merely contains schematic information about included tables and their
  // ownership and accessorship.
  std::unordered_map<ShardKind, Shard> shards_;

  // All the tables defined by the schema.
  std::unordered_map<TableName, Table> tables_;

  // Counts of users currently in the system.
  std::unordered_map<ShardKind, std::atomic<uint64_t>> users_;
  std::atomic<uint64_t> index_count_;
};

}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_STATE_H_
