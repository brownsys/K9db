#include "pelton/shards/types.h"

namespace pelton {
namespace shards {

std::ostream &operator<<(std::ostream &os, const DirectInfo &o) {
  os << "`" << o.column << "`";
  return os;
}

std::ostream &operator<<(std::ostream &os, const TransitiveInfo &o) {
  os << "`" << o.column << "` -> `" << o.next_table << "`.`" << o.next_column
     << "`";
  return os;
}

std::ostream &operator<<(std::ostream &os, const VariableInfo &o) {
  os << "`" << o.origin_relation << "`.`" << o.origin_column << "` --> `"
     << o.column << "`";
  return os;
}

std::ostream &operator<<(std::ostream &os, const ShardDescriptor &o) {
  os << "{`" << o.shard_kind << "`: ";
  switch (o.type) {
    case DIRECT:
      os << std::get<DirectInfo>(o.info);
      break;
    case TRANSITIVE:
      os << std::get<TransitiveInfo>(o.info);
      break;
    case VARIABLE:
      os << std::get<VariableInfo>(o.info);
      break;
  }
  os << "}";
  return os;
}

std::ostream &operator<<(std::ostream &os, const Table &o) {
  os << "Table `" << o.table_name << "`: owners [";
  for (size_t i = 0; i < o.owners.size(); i++) {
    if (i > 0) {
      os << ", ";
    }
    os << o.owners.at(i);
  }
  os << "], accessors [";
  for (size_t i = 0; i < o.accessors.size(); i++) {
    if (i > 0) {
      os << ", ";
    }
    os << o.accessors.at(i);
  }
  os << "]";
  return os;
}

std::ostream &operator<<(std::ostream &os, const Shard &o) {
  os << "`" << o.shard_kind << "`: owns [";
  size_t i = 0;
  for (const std::string &table_name : o.owned_tables) {
    if (i++ > 0) {
      os << ", ";
    }
    os << table_name;
  }
  os << "], accesses [";
  for (const std::string &table_name : o.accessor_tables) {
    if (i++ > 0) {
      os << ", ";
    }
    os << table_name;
  }
  os << "]";
  return os;
}

}  // namespace shards
}  // namespace pelton
