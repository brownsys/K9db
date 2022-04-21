#include "pelton/shards/types.h"

#include <iomanip>

namespace pelton {
namespace shards {

std::ostream &operator<<(std::ostream &os, const ShardingInformation &info) {
  os << "Shardinginformation {";
  os << "shard_kind = " << std::quoted(info.shard_kind) << ", ";
  os << "sharded_table_name = " << std::quoted(info.sharded_table_name) << ", ";
  os << "shard_by = " << std::quoted(info.shard_by) << ", ";
  os << "shard_by_index = " << info.shard_by_index << ", ";
  os << "distance_from_shard = " << info.distance_from_shard << ", ";
  os << "next_table = " << std::quoted(info.next_table) << ", ";
  os << "next_column = " << std::quoted(info.next_column) << ", ";
  os << "next_index_name = " << std::quoted(info.next_index_name) << ", ";
  os << "is_varowned_ = " << (info.is_varowned_ ? "true" : "false") << " }";
  return os;
}

}  // namespace shards
}  // namespace pelton
