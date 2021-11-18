// Defines the state of our sharding adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#ifndef PELTON_SHARDS_STATE_H_
#define PELTON_SHARDS_STATE_H_

#include <list>
#include <memory>
#include <shared_mutex>
#include <string>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "pelton/shards/types.h"
#include "pelton/sql/lazy_executor.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {

class SharderStateLock {
  public:
    // Constructor to create a shared lock
    SharderStateLock(std::shared_lock<std::shared_mutex>&& reader_lock1,
                     std::shared_lock<std::shared_mutex>&& reader_lock2) :
      reader_lock1_(std::optional(std::move(reader_lock1))),
      reader_lock2_(std::optional(std::move(reader_lock2))) {
    }
    // Constructor to create an exclusive lock
    SharderStateLock(std::unique_lock<std::shared_mutex>&& writer_lock1,
                     std::unique_lock<std::shared_mutex>&& writer_lock2) :
      writer_lock1_(std::optional(std::move(writer_lock1))),
      writer_lock2_(std::optional(std::move(writer_lock2))) {
    }

    // Not copyable or movable.
    SharderStateLock(const SharderStateLock &) = delete;
    SharderStateLock &operator=(const SharderStateLock &) = delete;
    SharderStateLock(const SharderStateLock &&) = delete;
    SharderStateLock &operator=(const SharderStateLock &&) = delete;

    // No need for an explicit destructor; locks will be unlocked when
    // the SharderStateLock is destroyed.

    bool IsExclusive() {
      CHECK(writer_lock1_.has_value() == writer_lock2_.has_value());
      return writer_lock1_.has_value();
    }

    void Unlock() {
      if (writer_lock1_.has_value()) {
        writer_lock1_.value().unlock();
        writer_lock2_.value().unlock();
      } else {
        reader_lock1_.value().unlock();
        if (reader_lock2_.has_value()) {
          reader_lock2_.value().unlock();
        } else {
          // upgraded lock
          writer_lock2_.value().unlock();
        }
      }
    }

    void UnlockInner() {
      CHECK(reader_lock1_.has_value()) << "Sharder state must be locked for reading to upgrade";
      reader_lock2_.value().unlock();
    }

    void UpgradeInner(std::unique_lock<std::shared_mutex>&& writer_lock) {
      writer_lock2_ = std::optional(std::move(writer_lock));
      reader_lock2_.reset();
    }

  private:
    std::optional<std::shared_lock<std::shared_mutex>> reader_lock1_;
    std::optional<std::shared_lock<std::shared_mutex>> reader_lock2_;
    std::optional<std::unique_lock<std::shared_mutex>> writer_lock1_;
    std::optional<std::unique_lock<std::shared_mutex>> writer_lock2_;
};

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

  const std::unordered_set<UnshardedTableName> &TablesInShard(
      const ShardKind &kind) const;

  // Manage secondary indices.
  bool HasIndexFor(const UnshardedTableName &table_name,
                   const ColumnName &column_name,
                   const ColumnName &shard_by) const;

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

  // Save state to durable file.
  void Save(const std::string &dir_path);

  // Load state from its durable file (if exists).
  void Load(const std::string &dir_path);

  // Synchronization of state access is a responsibility of the caller.
  // Caller will unlock automatically once the returned lock goes out of scope.
  SharderStateLock LockShared() {
    std::shared_lock<std::shared_mutex> reader_lock(this->metadata_mtx1_);
    std::shared_lock<std::shared_mutex> reader_lock2(this->metadata_mtx2_);
    return SharderStateLock(std::move(reader_lock), std::move(reader_lock2));
  }
  SharderStateLock LockUnique() {
    std::unique_lock<std::shared_mutex> writer_lock(this->metadata_mtx1_);
    std::unique_lock<std::shared_mutex> writer_lock2(this->metadata_mtx2_);
    return SharderStateLock(std::move(writer_lock), std::move(writer_lock2));
  }
  void LockUpgrade(SharderStateLock& lock) {
    // Precondition: metadata_mtx1_ and metadata_mtx2_ are locked for reading
    // by this thread, but upgrade_mtx_ is not locked.
    //
    // 1. acquire upgrade mutex.
    std::unique_lock<std::mutex> upgrade_lock(this->upgrade_mtx_);
    // 2. unlock metadata_mtx2_ for reading. This is necessarily to re-lock it
    //    with exclusive access. This is fine and excludes
    //    other writers, since no other writer can hold metadata_mtx1_ with
    //    an exclusive lock, and we're not unlocking it.
    //    NOTE: after this line, another thread could acquire
    //    metadata_mtx2_, as we might get preempted. But this is fine:
    //    a) for a reader, the unique_lock below will block until the
    //       reader is gone;
    //    b) no writer can lock metadata_mtx2_, because it would need to
    //       acquire metadata_mtx1_ first, but we have that locked exclusively;
    //    c) no other upgrading writer can lock metadata_mtx2_ as it would
    //       need to acquire upgrade_mtx_ first, and we have that mutex
    //       locked, so it must wait.
    lock.UnlockInner();
    // 3. re-lock metadata_mtx_ for writing, while continuing to hold the lock
    //    on upgrade_mtx_.
    std::unique_lock<std::shared_mutex> writer_lock(this->metadata_mtx2_);
    // Postcondition: BOTH upgrade_mtx_ AND metadata_mtx2_ are locked exclusively
    // by this thread. Caller must unlock these mutexes.
    lock.UpgradeInner(std::move(writer_lock));
  }

  // Returns number of shards in a synchronized way. Callers need not lock the
  // state prior to calling this method.
  size_t NumShards() {
    std::shared_lock<std::shared_mutex> lock(this->metadata_mtx1_);
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

  // All columns in a table that have an index.
  std::unordered_map<UnshardedTableName, std::unordered_set<ColumnName>>
      indices_;

  // table-name -> column with an index name -> shard by column for which
  //    the index provide values -> the corresponding flow name of the index.
  std::unordered_map<
      UnshardedTableName,
      std::unordered_map<ColumnName, std::unordered_map<ColumnName, FlowName>>>
      index_to_flow_;

  // empty set to return in SharderState::IndicesFor if table not found
  // needed to keep IndicesFor a const function (reader)
  std::unordered_set<ColumnName> empty_columns;

  // reader/writer locks to protect sharder state against concurrent
  // modification when there are multiple clients. We need two of them
  // to implement the safe upgrade protocol in LockUpgrade().
  mutable std::shared_mutex metadata_mtx1_;
  mutable std::shared_mutex metadata_mtx2_;
  // another mutex is needed so that we can "upgrade" the reader locks to
  // unique locks when it turns out that an operation needs to modify the
  // sharder metadata (e.g., updates and inserts). This lock excludes
  // other upgraders and avoids a race between upgraders.
  mutable std::mutex upgrade_mtx_;
};

}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_STATE_H_
