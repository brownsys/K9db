#ifndef PELTON_SHARDS_TYPES_H_
#define PELTON_SHARDS_TYPES_H_

#include <functional>
#include <string>
#include <vector>

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
        next_index_name("") {}

  // A transitive sharding information can only be created given the previous
  // sharding information in the transitivity chain.
  bool MakeTransitive(const ShardingInformation &next, const FlowName &index) {
    distance_from_shard = next.distance_from_shard + 1;
    next_table = shard_kind;
    shard_kind = next.shard_kind;
    next_index_name = index;
    // Cannot support deeply transitive things yet.
    return distance_from_shard == 1;
  }

  // Helpers.
  bool IsTransitive() const { return this->distance_from_shard > 0; }
};

}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_TYPES_H_
