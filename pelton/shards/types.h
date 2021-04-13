#ifndef PELTON_SHARDS_TYPES_H_
#define PELTON_SHARDS_TYPES_H_

#include <functional>
#include <string>
#include <vector>

namespace pelton {
namespace shards {

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

}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_TYPES_H_
