#include "pelton/shards/types.h"

#include "glog/logging.h"

namespace pelton {
namespace shards {

std::ostream &operator<<(std::ostream &os, const InfoType &o) {
  switch (o) {
    case InfoType::DIRECT:
      os << "DIRECT";
      break;
    case InfoType::TRANSITIVE:
      os << "TRANSITIVE";
      break;
    case InfoType::VARIABLE:
      os << "VARIABLE";
      break;
    default:
      LOG(FATAL) << "Unreachable";
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, const ShardDescriptor &o) {
  os << "{ \n";
  os << "  shard_kind: " << o.shard_kind << ",\n";
  os << "  type: " << o.type << ",\n";
  os << "  downcolumn: " << o.downcolumn() << ",\n";
  os << "  upcolumn: " << o.next_table() << "." << o.upcolumn() << "\n";
  os << "}";
  return os;
}

std::ostream &operator<<(std::ostream &os, const Table &o) {
  os << "Table `" << o.table_name << "`: owners [";
  for (size_t i = 0; i < o.owners.size(); i++) {
    if (i > 0) {
      os << ", ";
    }
    os << *o.owners.at(i);
  }
  os << "], accessors [";
  for (size_t i = 0; i < o.accessors.size(); i++) {
    if (i > 0) {
      os << ", ";
    }
    os << *o.accessors.at(i);
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

// Helpers for accessing members of InfoType in a type agnostic way.
const TableName &ShardDescriptor::next_table() const {
  switch (this->type) {
    case InfoType::DIRECT:
      return this->shard_kind;
    case InfoType::VARIABLE:
      return std::get<VariableInfo>(this->info).origin_relation;
    case InfoType::TRANSITIVE:
      return std::get<TransitiveInfo>(this->info).next_table;
    default:
      LOG(FATAL) << "Unreachable";
  }
}
const ColumnName &ShardDescriptor::downcolumn() const {
  switch (this->type) {
    case InfoType::DIRECT:
      return std::get<DirectInfo>(this->info).column;
    case InfoType::TRANSITIVE:
      return std::get<TransitiveInfo>(this->info).column;
    case InfoType::VARIABLE:
      return std::get<VariableInfo>(this->info).column;
    default:
      LOG(FATAL) << "Unreachable";
  }
}
const ColumnName &ShardDescriptor::upcolumn() const {
  switch (this->type) {
    case InfoType::DIRECT:
      return std::get<DirectInfo>(this->info).next_column;
    case InfoType::TRANSITIVE:
      return std::get<TransitiveInfo>(this->info).next_column;
    case InfoType::VARIABLE:
      return std::get<VariableInfo>(this->info).origin_column;
    default:
      LOG(FATAL) << "Unreachable";
  }
}
size_t ShardDescriptor::upcolumn_index() const {
  switch (this->type) {
    case InfoType::DIRECT:
      return std::get<DirectInfo>(this->info).next_column_index;
    case InfoType::TRANSITIVE:
      return std::get<TransitiveInfo>(this->info).next_column_index;
    case InfoType::VARIABLE:
      return std::get<VariableInfo>(this->info).origin_column_index;
    default:
      LOG(FATAL) << "Unreachable";
  }
}
const std::optional<IndexDescriptor *> ShardDescriptor::index_descriptor()
    const {
  switch (this->type) {
    case InfoType::DIRECT:
      return {};
    case InfoType::TRANSITIVE:
      return std::get<TransitiveInfo>(this->info).index;
    case InfoType::VARIABLE:
      return {};
    default:
      LOG(FATAL) << "Unreachable";
  }
}

}  // namespace shards
}  // namespace pelton
