// This file contains utility functions, mostly for standarizing
// name (suffixes etc) of things.

#ifndef PELTON_SHARDS_SQLENGINE_UTIL_H_
#define PELTON_SHARDS_SQLENGINE_UTIL_H_

#include <string>

#include "absl/strings/str_cat.h"
#include "pelton/shards/types.h"

namespace pelton {
namespace shards {
namespace sqlengine {

inline ShardedTableName NameShardedTable(const UnshardedTableName &table_name,
                                         const ColumnName &shard_by_column) {
  return absl::StrCat(table_name, "_", shard_by_column);
}

inline std::string NameShard(const ShardKind &shard_kind,
                             const UserId &user_id) {
  return absl::StrCat(shard_kind, "_", user_id, ".sqlite3");
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_UTIL_H_
