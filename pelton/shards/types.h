#ifndef PELTON_SHARDS_TYPES_H_
#define PELTON_SHARDS_TYPES_H_

#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "pelton/dataflow/schema.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {

#define EXTRACT_VARIANT(prop, info) \
  visit([](const auto &arg) { return arg.prop; }, info)

// The name of a dataflow.
using FlowName = std::string;

// The name of the table representing users that own shards of this kind.
using ShardKind = std::string;

// The name of a table (sharded or not).
using TableName = std::string;

// A unique identifier that represents a user.
// This is used to come up with shard names for new users, and to track
// which shard belongs to which user.
using UserID = std::string;

using ColumnName = std::string;

using ColumnIndex = size_t;

// Index information.
enum class IndexType { SIMPLE, JOINED };
struct SimpleIndexInfo {
  ColumnName shard_column;
};
struct JoinedIndexInfo {
  TableName join_table;     // could also be another index/flow.
  ColumnName join_column1;  // column in indexed table.
  ColumnName join_column2;  // column in join table.
  ColumnName shard_column;  // Final shard column, in join table.
};

// An index is either simple (over a single table) or joined (with another table
// or index).
struct IndexDescriptor {
  FlowName index_name;
  TableName indexed_table;
  ColumnName indexed_column;
  IndexType type;
  std::variant<SimpleIndexInfo, JoinedIndexInfo> info;
};

// Forward declaration.
struct ShardDescriptor;

// Types of a sharded table.
// We also use these to describe accessorship as well.
// For accessors, indices are fliped: they data subject ID to
// the row in the table being described.
enum class InfoType { DIRECT, TRANSITIVE, VARIABLE };
struct DirectInfo {
  // The FK column with OWNER/ACCESSOR annotation.
  // For data subject tables, this is the PK.
  // Invariant: value of this column equals to ID of owner.
  ColumnName column;
  ColumnIndex column_index;
  sqlast::ColumnDefinition::Type column_type;
};
struct TransitiveInfo {
  // The FK column with OWNER/ACCESSOR annotation that transitively leads to the
  // data subject table.
  ColumnName column;
  ColumnIndex column_index;
  sqlast::ColumnDefinition::Type column_type;
  // The next hop table in the transitive relationship eventually leading to
  // data subject. Following next hops recursively until transitivity is
  // consumed (plus on more step for the direct table) must lead to the data
  // subject table.
  ShardDescriptor *next;
  TableName next_table;
  ColumnName next_column;
  ColumnIndex next_column_index;
  // The index for dealing with transitivity. It implicitly joins over
  // transitive hops.
  // The index maps values of <column> to data subject IDs (for ownership).
  // No index for accessors: this is empty.
  std::optional<IndexDescriptor> index;
};
struct VariableInfo {
  // The column in this table that the variable ownership/accessorship table
  // points to via an FK with the OWNS/ACCESSES annotation. Usually the PK.
  ColumnName column;
  ColumnIndex column_index;
  sqlast::ColumnDefinition::Type column_type;
  // The variable ownership/accessorship table, and the FK column it has that
  // points to this table (to <column> specifically).
  ShardDescriptor *next;
  TableName origin_relation;
  ColumnName origin_column;
  ColumnIndex origin_column_index;
  // The index for dealing with variability.
  // The index maps values of <column> to data subject IDs (for ownership).
  // No index for accessors: this is empty.
  std::optional<IndexDescriptor> index;
};

// Specifies one way a table is sharded.
struct ShardDescriptor {
  // The kind of users this sharding associate the table with.
  ShardKind shard_kind;
  // The type of sharding.
  InfoType type;
  std::variant<DirectInfo, TransitiveInfo, VariableInfo> info;
};

// Metadata about a sharded table.
struct Table {
  TableName table_name;
  dataflow::SchemaRef schema;
  sqlast::CreateTable create_stmt;
  // owners.size() > 0 <=> sharded table.
  std::vector<std::unique_ptr<ShardDescriptor>> owners;
  std::vector<std::unique_ptr<ShardDescriptor>> accessors;
};

// Metadata about a shard.
struct Shard {
  // The kind of users this shard represents (e.g. User, Doctor, Patient, etc),
  // it's name of the data subject table.
  ShardKind shard_kind;
  ColumnName id_column;
  ColumnIndex id_column_index;
  // All (sharded) tables included in this shard.
  std::unordered_set<TableName> owned_tables;
  // All tables that may have be accessed from this shard.
  std::unordered_set<TableName> accessor_tables;
};

// For debugging / printing.
std::ostream &operator<<(std::ostream &os, const SimpleIndexInfo &o);
std::ostream &operator<<(std::ostream &os, const JoinedIndexInfo &o);
std::ostream &operator<<(std::ostream &os, const IndexDescriptor &o);
std::ostream &operator<<(std::ostream &os, const DirectInfo &o);
std::ostream &operator<<(std::ostream &os, const TransitiveInfo &o);
std::ostream &operator<<(std::ostream &os, const VariableInfo &o);
std::ostream &operator<<(std::ostream &os, const ShardDescriptor &o);
std::ostream &operator<<(std::ostream &os, const Table &o);
std::ostream &operator<<(std::ostream &os, const Shard &o);

}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_TYPES_H_
