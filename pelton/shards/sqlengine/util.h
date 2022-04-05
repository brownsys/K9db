// This file contains utility functions, mostly for standarizing
// name (suffixes etc) of things.

#ifndef PELTON_SHARDS_SQLENGINE_UTIL_H_
#define PELTON_SHARDS_SQLENGINE_UTIL_H_

#include <string>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "pelton/shards/types.h"
#include "pelton/sqlast/ast_schema.h"

namespace pelton {
namespace shards {
namespace sqlengine {

enum OwningT {
  OWNING,
  ACCESSING,
};

inline ShardedTableName NameShardedTable(const UnshardedTableName &table_name,
                                         const ColumnName &shard_by_column) {
  return absl::StrCat(table_name, "_", shard_by_column);
}

inline std::optional<OwningT> IsOwning(const sqlast::ColumnDefinition &column) {
  if (absl::StartsWith(column.column_name(), "OWNING_"))
    return OwningT::OWNING;
  else if (absl::StartsWith(column.column_name(), "ACCESSING_"))
    return OwningT::ACCESSING;
  else
    return std::optional<OwningT>{};
}

std::string Dequote(const std::string &st);

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_UTIL_H_
