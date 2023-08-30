#ifndef K9DB_SHARDS_TYPES_H_
#define K9DB_SHARDS_TYPES_H_

// NOLINTNEXTLINE
#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "k9db/dataflow/schema.h"
#include "k9db/sqlast/ast.h"

namespace k9db {
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

// Index descriptor.
// An in-memory secondary index over a given table.
// The index maps values of the specified columns to a list of owners from a
// particular user/shard kind that own any record in this table with that value.
struct IndexDescriptor {
  FlowName index_name;
  TableName table_name;
  ShardKind shard_kind;
  ColumnName column_name;
};

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
  // The referenced column in the data subject table.
  ColumnName next_column;
  ColumnIndex next_column_index;
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
  TableName next_table;
  ColumnName next_column;
  ColumnIndex next_column_index;
  // The index for dealing with transitivity. It implicitly joins over
  // transitive hops.
  // The index maps values of <column> to data subject IDs (for ownership).
  // No index for accessors: this is empty.
  IndexDescriptor *index;
};
struct VariableInfo {
  // The column in this table that the variable ownership/accessorship table
  // points to via an FK with the OWNS/ACCESSES annotation. Usually the PK.
  ColumnName column;
  ColumnIndex column_index;
  sqlast::ColumnDefinition::Type column_type;
  // The variable ownership/accessorship table, and the FK column it has that
  // points to this table (to <column> specifically).
  TableName origin_relation;
  ColumnName origin_column;
  ColumnIndex origin_column_index;
};

// Specifies one way a table is sharded.
struct ShardDescriptor {
  // The kind of users this sharding associate the table with.
  ShardKind shard_kind;
  // The type of sharding.
  InfoType type;
  std::variant<DirectInfo, TransitiveInfo, VariableInfo> info;

  bool is_transitive() const { return this->type == InfoType::TRANSITIVE; }
  bool is_varowned() const { return this->type == InfoType::VARIABLE; }

  // `upcolumn` and `downcolumn` are just convenient ways to access the
  // "arrowheads" on an ownership relationship. Together with `next_table` this
  // lets you talk about the entire relationship with the next "hop" in the
  // following way: Lets assume we have a table `t` with a shard descriptor `d`.
  //
  // +----------------+        +----------------+
  // | d.next_table() |        | t              |
  // +----------------+        +----------------+
  // | d.upcolumn()   | <----- | d.downcolumn() |
  // | ...            |        | ...            |
  // +----------------+        +----------------+
  //
  // `d.next_table()` always returns a table that is one hop closer in the
  // transitivity chain. The arrow between up- and downcolumn can be reversed
  // (in the case of variable ownership), however `downcolumn` will always be
  // the column in `t` and `upcolumn` always be the column in `d.next_table()`
  // regardless of arrow direction.
  const ColumnName &downcolumn() const;
  const ColumnName &upcolumn() const;
  size_t upcolumn_index() const;
  const TableName &next_table() const;
  const std::optional<IndexDescriptor *> index_descriptor() const;
};

// Metadata about a sharded table.
struct Table {
  TableName table_name;
  dataflow::SchemaRef schema;
  sqlast::CreateTable create_stmt;
  // Auto increment and defaults.
  std::unique_ptr<std::atomic<int64_t>> counter;
  std::unordered_set<ColumnName> auto_increments;
  std::unordered_map<ColumnName, sqlast::Value> defaults;
  // owners.size() > 0 <=> sharded table.
  std::vector<std::unique_ptr<ShardDescriptor>> owners;
  std::vector<std::unique_ptr<ShardDescriptor>> accessors;
  // Dependent tables: the placement of rows in these tables depend
  // on rows in this table (e.g. owned by a transitive FK into this table).
  std::vector<std::pair<TableName, ShardDescriptor *>> dependents;
  std::vector<std::pair<TableName, ShardDescriptor *>> access_dependents;
  // All indices created for this table.
  std::unordered_map<
      ColumnName,
      std::unordered_map<ShardKind, std::unique_ptr<IndexDescriptor>>>
      indices;
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
std::ostream &operator<<(std::ostream &os, const InfoType &o);
std::ostream &operator<<(std::ostream &os, const ShardDescriptor &o);
std::ostream &operator<<(std::ostream &os, const Table &o);
std::ostream &operator<<(std::ostream &os, const Shard &o);

// Helper to allow for easily traversing a tree of owned/accessed tables.
// void visit(owner, owned, desc, A).
template <typename A>
class Visitor {
 private:
  using State = std::unordered_map<TableName, Table>;
  using Up = const Table &;
  using Down = const Table &;
  using Link = const ShardDescriptor *;
  using Functor = std::function<bool(Up, Down, Link, A)>;

 public:
  // Use this if you want to instantiate by passing in a lambda.
  explicit Visitor(Functor f) : f_(std::move(f)) {}

 private:
  // Invoked by SharderState to visit.
  void Initialize(const State *state) { this->state_ = state; }

  // Internal; Do not modify or override.
  template <bool Up>
  void VisitOwners(const TableName &table_name, A arg) {
    const Table &table = this->state_->at(table_name);
    // Identify direction.
    if constexpr (Up) {
      for (const auto &desc : table.owners) {
        if (desc->shard_kind != table_name) {
          const Table &next = this->state_->at(desc->next_table());
          if (this->VisitOwner(next, table, desc.get(), arg)) {
            this->VisitOwners<true>(next.table_name, arg);
          }
        }
      }
    } else {
      for (const auto &[next_table, desc] : table.dependents) {
        const Table &next = this->state_->at(next_table);
        if (this->VisitOwner(table, next, desc, arg)) {
          this->VisitOwners<false>(next.table_name, arg);
        }
      }
    }
  }
  template <bool Up>
  void VisitAccessors(const TableName &table_name, A arg) {
    const Table &table = this->state_->at(table_name);
    // Identify direction.
    if constexpr (Up) {
      for (const auto &desc : table.accessors) {
        const Table &next = this->state_->at(desc->next_table());
        if (this->VisitAccessor(next, table, desc.get(), arg)) {
          this->VisitAccessors<true>(next.table_name, arg);
        }
      }
    } else {
      for (const auto &[next_table, desc] : table.access_dependents) {
        const Table &next = this->state_->at(next_table);
        if (this->VisitAccessor(table, next, desc, arg)) {
          this->VisitAccessors<false>(next.table_name, arg);
        }
      }
    }
  }

 protected:
  // If you want to instantiate by overriding/inheritance, then your subclass
  // should invoke this default constructor, and then override Visit(...) below.
  Visitor() = default;
  virtual bool VisitOwner(Up u, Down d, Link l, A a) {
    return this->f_.value()(u, d, l, a);
  }
  virtual bool VisitAccessor(Up u, Down d, Link l, A a) {
    return this->f_.value()(u, d, l, a);
  }

 private:
  std::optional<Functor> f_;
  const State *state_ = nullptr;
  friend class SharderState;
};

}  // namespace shards
}  // namespace k9db

#endif  // K9DB_SHARDS_TYPES_H_
