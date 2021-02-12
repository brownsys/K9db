// Defines the state of our adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#ifndef PELTON_SHARDS_STATE_H_
#define PELTON_SHARDS_STATE_H_

#include <cstring>
#include <functional>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/schema.h"
#include "pelton/shards/sqlexecutor/executor.h"
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

// The name of the table representing users that own shards of this kind.
using ShardKind = std::string;

// The name of a table from the original unsharded schema.
using UnshardedTableName = std::string;

// The name of a table in the sharded schema.
// Usually, this is the name of the original table this table corresponds to,
// but has the sharding column name appended to it as a suffix.
// This could also be exactly the same as the unsharded name if the table
// is not sharded.
using ShardedTableName = std::string;

// A unique identifier that represents a user.
// This is used to come up with shard names for new users, and to track
// which shard belongs to which user.
using UserId = std::string;

using ColumnName = std::string;

using ColumnIndex = size_t;

using FlowName = std::string;

// Valid SQL CreateTable statement formatted and ready to use to create
// some table.
using CreateStatement = std::string;

// Contains the details of how a given table is sharded.
struct ShardingInformation {
  // Which shard does this belong to (e.g. User, Doctor, Patient, etc)
  ShardKind shard_kind;
  // Name of the table after sharding.
  ShardedTableName sharded_table_name;
  // The column the table is sharded by. This is a column from the unsharded
  // schema that is removed post-sharding, it can be deduced from the shard
  // name.
  ColumnName shard_by;
  // The index of the sharding column in the table definition.
  ColumnIndex shard_by_index;
};

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
  void AddShardKind(const ShardKind &kind, const ColumnName &pk);

  void AddUnshardedTable(const UnshardedTableName &table,
                         const CreateStatement &create_str,
                         const dataflow::Schema &schema);

  void AddShardedTable(const UnshardedTableName &table,
                       const ShardingInformation &sharding_information,
                       const CreateStatement &sharded_create_statement,
                       const dataflow::Schema &schema);

  std::list<CreateStatement> CreateShard(const ShardKind &shard_kind,
                                         const UserId &user);

  void RemoveUserFromShard(const ShardKind &kind, const UserId &user_id);

  // Schema lookups.
  bool Exists(const UnshardedTableName &table) const;

  CreateStatement ConcreteSchemaOf(const ShardedTableName &table) const;

  const dataflow::Schema &LogicalSchemaOf(
      const UnshardedTableName &table) const;

  bool IsSharded(const UnshardedTableName &table) const;

  const std::list<ShardingInformation> &GetShardingInformation(
      const UnshardedTableName &table) const;

  bool IsPII(const UnshardedTableName &table) const;

  const ColumnName &PkOfPII(const UnshardedTableName &table) const;

  bool ShardExists(const ShardKind &shard_kind, const UserId &user) const;

  const std::unordered_set<UserId> &UsersOfShard(const ShardKind &kind) const;

  // Save state to durable file.
  void Save();

  // Load state from its durable file (if exists).
  void Load();

  // Return the connection pool to use for executing statements.
  sqlexecutor::SQLExecutor *SQLExecutor();

  // Add and manage flows.
  void AddFlow(const FlowName &name, const dataflow::DataFlowGraph &flow);

  const dataflow::DataFlowGraph &GetFlow(const FlowName &name) const;

  bool HasInputsFor(const UnshardedTableName &table_name) const;

  const std::vector<std::shared_ptr<dataflow::InputOperator>> &InputsFor(
      const UnshardedTableName &table_name) const;

 private:
  // Directory in which all shards are stored.
  std::string dir_path_;

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

  // Maps every table in the overall schema to its concrete schema.
  // This can be used to create that table in a new shard.
  // The concrete schema matches what is stored physically in the DB after
  // sharding and other transformations.
  std::unordered_map<ShardedTableName, CreateStatement> concrete_schema_;

  // Maps every table to its logical schema.
  // The logical schema is the contract between client code and our DB.
  // The stored schema may not matched the concrete/physical one due to sharding
  // or other transformations.
  std::unordered_map<UnshardedTableName, dataflow::Schema> logical_schema_;

  // Connection pool that manages the underlying sqlite3 databases.
  sqlexecutor::SQLExecutor executor_;

  // Dataflow graphs and views.
  std::unordered_map<FlowName, dataflow::DataFlowGraph> flows_;
  std::unordered_map<UnshardedTableName,
                     std::vector<std::shared_ptr<dataflow::InputOperator>>>
      inputs_;
};

}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_STATE_H_
