// This file contains utility functions, mostly for standarizing
// name (suffixes etc) of things.

#ifndef SHARDS_SQLENGINE_UTIL_H_
#define SHARDS_SQLENGINE_UTIL_H_

#define DEFAULT_SHARD_NAME "default"

#include <string>

#include "absl/strings/str_cat.h"
#include "shards/state.h"

namespace shards {
namespace sqlengine {

inline ShardedTableName NameShardedTable(
    const UnshardedTableName &table_name,
    const ColumnName &shard_by_column) {
  return absl::StrCat(table_name, "_", shard_by_column);
}

inline std::string NameShard(
    const ShardKind &shard_kind,
    const UserId &user_id) {
  return absl::StrCat(shard_kind, "_", user_id);
}

}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_UTIL_H_
