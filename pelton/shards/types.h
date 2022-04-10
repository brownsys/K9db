#ifndef PELTON_SHARDS_TYPES_H_
#define PELTON_SHARDS_TYPES_H_

#include <functional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "glog/logging.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {

// The name of a dataflow.
using FlowName = std::string;

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

using IndexName = std::string;

// Contains the information mapping an accessor to the relevant tables and
// columns
// Used to construct query of the form:
// SELECT * FROM table WHERE accessor = id AND owner IN (...)
struct AccessorIndexInformation {
  // Which shard has access to the information in the table
  // (e.g. User, Doctor, Patient, etc)
  ShardKind shard_kind;
  // The table which the index is created on
  UnshardedTableName table_name;
  // The column which the accessor notation is defined on
  ColumnName accessor_column_name;
  // Table from which this is acessed, if not PII
  std::optional<UnshardedTableName> foreign_table;
  // The column which the table is sharded on
  ColumnName shard_by_column_name;
  // The name of the index
  IndexName index_name;
  // columns to anonymize on delete and their types
  std::unordered_map<ColumnName, sqlast::ColumnDefinition::Type>
      anonymize_columns;
  bool is_sharded;

  AccessorIndexInformation(
      const ShardKind &sk, const UnshardedTableName &tn, const ColumnName &cn,
      const UnshardedTableName &ftab, const ColumnName &sbcn,
      const IndexName &in,
      const std::unordered_map<ColumnName, sqlast::ColumnDefinition::Type> &an,
      const bool is)
      : shard_kind(sk),
        table_name(tn),
        accessor_column_name(cn),
        foreign_table(ftab),
        shard_by_column_name(sbcn),
        index_name(in),
        anonymize_columns(an),
        is_sharded(is) {}

  AccessorIndexInformation(
      const ShardKind &sk, const UnshardedTableName &tn, const ColumnName &cn,
      const ColumnName &sbcn, const IndexName &in,
      const std::unordered_map<ColumnName, sqlast::ColumnDefinition::Type> &an,
      const bool is)
      : shard_kind(sk),
        table_name(tn),
        accessor_column_name(cn),
        foreign_table(),
        shard_by_column_name(sbcn),
        index_name(in),
        anonymize_columns(an),
        is_sharded(is) {}
};

// Contains the details of how a given table is sharded.
struct ShardingInformation {
  // Which shard does this belong to (e.g. User, Doctor, Patient, etc)
  ShardKind shard_kind;
  // Name of the table after sharding.
  ShardedTableName sharded_table_name;
  // The column the table is sharded by. This is a column from the unsharded
  // schema that is removed post-sharding, it can be deduced from the shard
  // name.
  // Note: in case of transitive sharding, this column is not removed, but its
  // value is used to look up the transitive sharding value via an index.
  ColumnName shard_by;
  // The index of the sharding column in the table definition.
  ColumnIndex shard_by_index;
  // 0 means direct sharding, 1 and above means a transitive shard.
  // if this value is i, it means that there are i foreign key hops between
  // this table and the initial shard_by column leading to a PII.
  int distance_from_shard;
  // In case of transitive sharding (distance_from_shard > 0), these fields
  // store the name of the next intermediate table and column.
  UnshardedTableName next_table;
  ColumnName next_column;
  FlowName next_index_name;
  FlowName owned_by_index_name;

  bool is_varowned_;
  bool is_owning_;

  // Constructors.
  ShardingInformation() = default;

  // Create a non transitive sharding information.
  ShardingInformation(const ShardKind &sk, const ShardedTableName &stn,
                      const ColumnName &sb, ColumnIndex sbi,
                      const ColumnName &nc)
      : shard_kind(sk),
        sharded_table_name(stn),
        shard_by(sb),
        shard_by_index(sbi),
        distance_from_shard(0),
        next_table(""),
        next_column(nc),
        next_index_name(""),
        owned_by_index_name(""),
        is_varowned_(false),
        is_owning_(false) {}

  // A transitive sharding information can only be created given the previous
  // sharding information in the transitivity chain.
  bool MakeTransitive(const ShardingInformation &next, const FlowName &index,
                      const UnshardedTableName &original_table_name) {
    auto hash = std::hash<std::string>{};
    size_t my_hash =
        hash(next.sharded_table_name) ^ hash(next.shard_by) ^ hash(shard_by);
    sharded_table_name = original_table_name + "_" + std::to_string(my_hash);
    distance_from_shard = next.distance_from_shard + 1;
    next_table = shard_kind;
    shard_kind = next.shard_kind;
    next_index_name = index;
    // Cannot support deeply transitive things yet.
    // return distance_from_shard < 1;
    return true;
  }

  // Helpers.
  bool IsTransitive() const { return this->distance_from_shard > 0; }

  bool RequiresOwningIndex() const { return this->is_owning_; }

  // owned_by_index_name is the index in the OWNING table which maps the 
  // foreign key to the primary key in the table to allow us to resolve the
  // shard to insert a value into
  void MakeOwning(const FlowName &owning_index) { 
    this->owned_by_index_name = owning_index;
    this->is_owning_ = true; 
  }

  void MakeVarowned() { this->is_varowned_ = true; }

  bool is_varowned() const { return this->is_varowned_; }
};

}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_TYPES_H_
