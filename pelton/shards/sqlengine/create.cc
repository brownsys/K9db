// CREATE TABLE statements sharding and rewriting.

#include "pelton/shards/sqlengine/create.h"

#include <list>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/shards/upgradable_lock.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"
#include "glog/logging.h"

#include "pelton/shards/sqlengine/index.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace create {

namespace {

// Describes whether a foreign key is internal to a shard (between two tables
// of the same shard kind) or external (the two tables belong to different
// shard kinds).
enum ForeignKeyType {
  // External: the column is kept, but the foreign key constraint is removed.
  FK_EXTERNAL,
  // Internal: column and constraint are unchanged.
  FK_INTERNAL,
  // A foreign key to the PII table that is the shard_by column:
  // both column and constraint are removed, since column value is redundant.
  FK_REMOVED
};

// Describes what should happen to every foreign key in a given table.
using ForeignKeyShards = std::unordered_map<ColumnName, ForeignKeyType>;

// Checks if the create table statement specifies any PII columns.
bool HasPII(const sqlast::CreateTable &stmt) {
  for (const sqlast::ColumnDefinition &column : stmt.GetColumns()) {
    if (absl::StartsWith(column.column_name(), "PII_")) {
      return true;
    }
  }
  return false;
}

// Return the name of the PK column of the given table.
absl::StatusOr<std::string> GetPK(const sqlast::CreateTable &stmt) {
  bool found = false;
  std::string pk;
  // Inline PK constraint.
  for (const auto &col : stmt.GetColumns()) {
    for (const auto &constraint : col.GetConstraints()) {
      if (constraint.type() == sqlast::ColumnConstraint::Type::PRIMARY_KEY) {
        if (found) {
          return absl::InvalidArgumentError(
              "Multi-column Primary Keys are not supported!");
        }
        found = true;
        pk = col.column_name();
      }
    }
  }
  if (!found) {
    return absl::InvalidArgumentError("Table has no Primary Key!");
  }
  return pk;
}

// Figures out whether or not there is a potential sharding relationship
// along the given foreign key. Even if this function discovers such a relation,
// it may not be used if the user overrides with OWNER annotation, or because
// of various unsupported scenarios that are checked later (see
// IsShardingBySupported(...)).
std::optional<ShardKind> ShouldShardBy(
    const sqlast::ColumnConstraint &foreign_key, const SharderState &state) {
  // First, determine if the foreign table is in a shard or has PII.
  const std::string &foreign_table = foreign_key.foreign_table();
  bool foreign_sharded = state.IsSharded(foreign_table);
  bool foreign_pii = state.IsPII(foreign_table);

  // FK links to a shard, add it as either an explicit or implicit owner.
  if (foreign_pii || foreign_sharded) {
    return foreign_table;
  }

  return std::optional<ShardKind>{};
}


// Determine whether and how to shard by the given information.
// Particularly, this resolves transitive shards.
//
// I changed the signature here, because this can now have more than one result. 
// We can find a more efficient implementation later.
absl::StatusOr<std::list<ShardingInformation>> IsShardingBySupported(
  const ShardingInformation &info,
  const SharderState &state,
  const UnshardedTableName &original_table_name) 
{
  const std::string &foreign_table = info.shard_kind;
  bool foreign_sharded = state.IsSharded(foreign_table);
  bool foreign_pii = state.IsPII(foreign_table);

  std::list<ShardingInformation> ways_to_shard;

  if (foreign_pii && foreign_sharded) {
    return absl::InvalidArgumentError("Foreign key into a PII + sharded table");
  } else if (foreign_pii) {
    ways_to_shard.push_back(info);
  } else if (foreign_sharded) {

    for (const ShardingInformation &other : state.GetShardingInformation(foreign_table)) {
      ways_to_shard.emplace_back(info);
      ShardingInformation *info_c = &ways_to_shard.back();
      FlowName index;
      if (foreign_table == "oc_share" && original_table_name == "oc_files" && other.shard_by == "OWNER_share_with_group") {
        LOG(INFO) << "Detected the group share";
        index = "users_for_file_via_group";
      } else if (foreign_table == "oc_groups" && original_table_name == "oc_share" && other.shard_by == "gid") {
        index = "users_for_group";
      } else if (!state.HasIndexFor(foreign_table, info_c->next_column, other.shard_by)) {
        return absl::InvalidArgumentError(
            "Cannot have a transitive FK pointing to non-index column (table_name: " + original_table_name + ", shard_by: " + info_c->shard_by + ", foreign_table: " + foreign_table + ", next_column: " + info_c->next_column + ", other.shard_by: " + other.shard_by + ")");
      } else {
        index = state.IndexFlow(foreign_table, info_c->next_column, other.shard_by);
      }
      if (!info_c->MakeTransitive(other, index, original_table_name)) {
        return absl::InvalidArgumentError("Transitive FK is too deep");
      }
    }
  }

  return ways_to_shard;
}

struct OwningTable {
  sqlast::ColumnDefinition column;
  sqlast::ColumnConstraint fk_constraint;
  OwningT owning_type;

  OwningTable(const sqlast::ColumnDefinition &col, OwningT owning_type, const sqlast::ColumnConstraint &fcon)
    : column(col),
      fk_constraint(fcon),
      owning_type(owning_type) {}

};

// Figures out which shard kind this table should belong to.
// Currently, we expect one of two solutions:
// 1) a foreign key column is explicitly specified to be the column to shard by
//    via OWNER annotation.
// 2) if no OWNER is specified, we can deduce automatically what to shard by (if
//    at all). Currently, we support only a single foreign key to be linked
//    to a PII table or to another table in some shard. This table is assigned
//    to the shard defined by that PII table, or to the same shard that the
//    target table is in. Having multiple such foreign keys results in a runtime
//    error.
absl::StatusOr<std::pair<std::list<ShardingInformation>, std::optional<OwningTable>>> ShardTable(
    const sqlast::CreateTable &stmt, const SharderState &state) {
  // Result is empty by default.
  std::list<ShardingInformation> explicit_owners;
  std::list<ShardingInformation> implicit_owners;
  std::unordered_map<ColumnName, ColumnIndex> index_map;
  std::optional<OwningTable> owning_table;

  // get table name from ast representation of sql statement
  const std::string &table_name = stmt.table_name();
  // Check column definitions for inlined foreign key constraints.
  const auto &columns = stmt.GetColumns();
  for (size_t index = 0; index < columns.size(); index++) {
    // Record index of column for later constraint processing.
    const sqlast::ColumnDefinition &col = columns[index];
    const std::string &column_name = col.column_name();
    index_map.insert({column_name, index});
    // Find foreign key constraint (if exists).
    // check if column starts with OWNER_ (has an owner annotation)
    bool explicit_owner =
        absl::StartsWith(column_name, "OWNER_") || columns[index].owner();
    // check if column starts with ACCESSOR_ (has an accessor annotation)
    bool explicit_accessor = absl::StartsWith(column_name, "ACCESSOR_");
    // check if column has a foreign key constraint
    const sqlast::ColumnConstraint *fk_constraint = nullptr;
    for (const auto &constraint : col.GetConstraints()) {
      if (constraint.type() == sqlast::ColumnConstraint::Type::FOREIGN_KEY) {
        fk_constraint = &constraint;
        break;
      }
    }

    // Use fk_constraint and owner annotation (explicit_owner)
    // to determine whether and how to shard by this column.
    if (fk_constraint != nullptr) {
      // We have a foreign key constraint, check if we should shard by it.
      // pass ShouldShardBy the fk_constraint (linking to the other, foreign
      // table, and the current table state. Assign return to shard_kind
      std::optional<ShardKind> shard_kind =
          ShouldShardBy(*fk_constraint, state);
      if (!shard_kind.has_value() && (explicit_owner || explicit_accessor)) {
        return absl::InvalidArgumentError("non-sharding fk as OWNER|ACCESSOR");
      }
      if (shard_kind.has_value()) {
        const std::string &foreign_column = fk_constraint->foreign_column();
        std::string sharded_table_name =
            NameShardedTable(table_name, column_name);
        // FK links to a shard, add it as either an explicit or implicit owner.
        if (explicit_owner) {
          explicit_owners.emplace_back(shard_kind.value(), sharded_table_name,
                                       column_name, index, foreign_column);
        } else {
          if (!explicit_accessor) {
            implicit_owners.emplace_back(shard_kind.value(), sharded_table_name,
                                         column_name, index, foreign_column);
          }
        }
      }
      auto is_owning = IsOwning(col);
      if (is_owning) {
        LOG(INFO) << "Detected owning column " << column_name ;
        // This table also denotes a relationship to another (via explicit annotation)
        if (fk_constraint != nullptr) {
          if (owning_table) {
            return absl::InvalidArgumentError("Duplicate 'OWNING' column");
          }
          owning_table.emplace(col, *is_owning, *fk_constraint);
        } else {
          return absl::InvalidArgumentError("Non fk-column specified as 'OWNING'");
        }
      }
    } else if (explicit_owner) {
      // Not a foreign key but should be sharded explicitly.
      std::string sharded_table_name =
          NameShardedTable(table_name, column_name);
      explicit_owners.emplace_back(table_name, sharded_table_name, column_name,
                                   index, "");
    } else if (explicit_accessor) {
      return absl::InvalidArgumentError("Accessor on no foreign key");
    }
  }

  // If sharding is explicitly specified via OWNER, we are done.
  if (explicit_owners.size() > 0) {
    return std::make_pair(explicit_owners, owning_table);
  }

  // Sharding is implicitly deduced, only works if there is one candidate!
  if (implicit_owners.size() > 1) {
    return absl::InvalidArgumentError(
        "Table with several foreign keys to shards or PII!");
  }
  return std::make_pair(implicit_owners, owning_table);
}

const std::string &GetShardFor(const UnshardedTableName &table, const shards::SharderState &state) 
{
  if (state.IsSharded(table)) {
    const std::list<ShardingInformation> &sinfo = state.GetShardingInformation(table);
    if (sinfo.size() != 1) {
      LOG(FATAL) << "Expected only one sharding information, got " << sinfo.size();
    }
    return sinfo.front().shard_kind;
  } else if (state.IsPII(table)) {
    return table;
  }
  LOG(FATAL) << "Expected " << table << " to be sharded or PII";
}

// Factored out version of the acessor creation. This makes the `target_table`, specifically the version in `shard_string` accessible via `column_name`.
absl::Status MakeAccessible(
  Connection *connection, 
  const UnshardedTableName &foreign_table,
  const UnshardedTableName &table_name,
  const ColumnName &column_name,
  const std::unordered_map<ColumnName, sqlast::ColumnDefinition::Type> &anon_cols,
  UniqueLock *lock) 
{
  shards::SharderState *state = connection->state->sharder_state();
  bool is_sharded = state->IsSharded(table_name);
  LOG(INFO) << "Making " << table_name << " available via " << column_name << " -> "
     << foreign_table;
  const std::string &shard_string = GetShardFor(foreign_table, *state);
  if (table_name == "oc_share" && foreign_table == "oc_groups" && column_name == "ACCESSOR_share_with_group") {
    const auto &info = state->GetShardingInformation(table_name).front();
    const std::string &table_key = info.shard_by;
    const auto &index_name = "users_for_share_via_group";
    LOG(INFO) << "Found special case, installing index " << index_name;
    state->AddAccessorIndex(shard_string, table_name, column_name, foreign_table,
                            table_key, index_name,
                            anon_cols, is_sharded);
  }
  else if (is_sharded) {
    const auto &info = state->GetShardingInformation(table_name).front();
    const std::string &table_key = info.shard_by;

    size_t counter = 0;
    if (state->HasAccessorIndices(shard_string)) {
      counter = state->GetAccessorIndices(shard_string).size();
    }
    std::string index_prefix =
        "ref_" + shard_string + std::to_string(counter);
    sqlast::CreateIndex create_index_stmt{index_prefix, table_name,
                                          column_name};

    auto status = 
      pelton::shards::sqlengine::index::CreateIndexStateIsAlreadyLocked(
        create_index_stmt,
        connection, lock);
    if (!status.ok())
      return status.status();
    const IndexName index_name = index_prefix + "_" + table_key;
    LOG(INFO) << "Installing accessor index " << index_name << " for " << table_name << "(" << column_name <<
      ") -> " << foreign_table << " sharded_by " << shard_string << " (" << table_key << ")";
    state->AddAccessorIndex(shard_string, table_name, column_name,
                            table_key, index_name,
                            anon_cols, is_sharded);
  } else {
    state->AddAccessorIndex("", table_name, column_name, "", "",
                            anon_cols, is_sharded);
  }
  return absl::OkStatus();
}

std::unordered_map<ColumnName, sqlast::ColumnDefinition::Type> GetAnonCols(
  const sqlast::ColumnDefinition &column,
  const std::vector<sqlast::ColumnDefinition> &columns) 
{
  const auto &column_name = column.column_name();
  std::unordered_map<ColumnName, sqlast::ColumnDefinition::Type> anon_cols;
  if (absl::StartsWith(column_name, "ACCESSOR_ANONYMIZE")) {
    anon_cols[column_name] = column.column_type();
    for (const auto &acces_col_check : columns) {
      const std::string &access_col_name = acces_col_check.column_name();
      if (absl::StartsWith(access_col_name, "ANONYMIZE_")) {
        anon_cols[access_col_name] = acces_col_check.column_type();
      }
    }
  }
  return anon_cols;
}

absl::Status IndexAccessor(const sqlast::CreateTable &stmt, Connection *connection, UniqueLock *lock) {
  shards::SharderState *state = connection->state->sharder_state();

  // Result is empty by default.
  std::unordered_map<ColumnName, ColumnIndex> index_map;

  // get table name from ast representation of sql statement
  const std::string &table_name = stmt.table_name();
  // Check column definitions for inlined foreign key constraints.
  const auto &columns = stmt.GetColumns();

  for (size_t index = 0; index < columns.size(); index++) {
    // Record index of column for later constraint processing.
    const std::string &column_name = columns[index].column_name();
    // check if column starts with ACCESSOR_ (has an accessor annotation)
    if (absl::StartsWith(column_name, "ACCESSOR_")) {
      const sqlast::ColumnConstraint *fk_constraint = nullptr;
      for (const auto &constraint : columns[index].GetConstraints()) {
        // if type of constraint is a foreign key, assign value to fk_constraint
        if (constraint.type() == sqlast::ColumnConstraint::Type::FOREIGN_KEY) {
          fk_constraint = &constraint;
          break;
        }
      }
      // Find anonymized columns.

      const auto anon_cols = GetAnonCols(columns[index], stmt.GetColumns());

      CHECK_STATUS(MakeAccessible(connection, fk_constraint->foreign_table(), table_name, column_name, anon_cols, lock));
    }
  }
  return absl::OkStatus();
}


// Determine what should be done about a single foreign key in some
// sharded table.
absl::StatusOr<ForeignKeyType> ShardForeignKey(
    const std::string &foreign_key_column_name,
    const sqlast::ColumnConstraint &foreign_key,
    const ShardingInformation &sharding_information,
    const SharderState &state) {
  const std::string &foreign_table = foreign_key.foreign_table();

  // Options:
  // 1. Foreign key is what we are sharding by:
  //    FK is removed if the sharding is direct (points to a PII table).
  //    FK is internal if the sharding is indirect.
  if (foreign_key_column_name == sharding_information.shard_by) {
    if (state.IsSharded(foreign_table)) {
      return FK_INTERNAL;
    } else {
      return FK_REMOVED;
    }
  }
  // 2. Foreign key is to an unsharded table.
  //    FK is external, column is kept but constraint dropped.
  // 3. Foreign key is to a sharded table.
  //    We have no guarantees if this table belongs to the same shard,
  //    or a different shard but is non-owning. We have no choice but to
  //    consider the FK external. Column is kept but constraint is dropped.
  return FK_EXTERNAL;
}

// Determine what should be done about all foreign keys in a sharded table.
absl::StatusOr<ForeignKeyShards> ShardForeignKeys(
    const sqlast::CreateTable &stmt, const ShardingInformation &info,
    const SharderState &state) {
  // Store the sharded resolution for every foreign key.
  ForeignKeyShards result;
  // Check column definitions for inlined foreign key constraints.
  for (const auto &column : stmt.GetColumns()) {
    const std::string &colname = column.column_name();
    const sqlast::ColumnConstraint *fk_constraint = nullptr;
    for (const auto &constraint : column.GetConstraints()) {
      if (constraint.type() == sqlast::ColumnConstraint::Type::FOREIGN_KEY) {
        fk_constraint = &constraint;
        break;
      }
    }
    if (fk_constraint != nullptr) {
      ASSIGN_OR_RETURN(ForeignKeyType fk_type,
                       ShardForeignKey(colname, *fk_constraint, info, state));
      result.emplace(colname, fk_type);
    } else if ((absl::StartsWith(colname, "OWNER_") || column.owner()) &&
               colname == info.shard_by) {
      result.emplace(colname, FK_REMOVED);
    }
  }
  // Remove shard_by column and its constraints.
  return result;
}

// Updates the create table statement to reflect the sharding information in
// fk_shards. Specifically, all out of shard foreign key constraints are removed
// (but the columns are kept), and the column being sharded by is entirely
// remove.
sqlast::CreateTable UpdateTableSchema(sqlast::CreateTable stmt,
                                      const ForeignKeyShards &fk_shards,
                                      const std::string &sharded_table_name) {
  stmt.table_name() = sharded_table_name;
  // Drop columns marked with FK_REMOVED, and fk constraints of columns marked
  // with FK_EXTERNAL.
  for (const auto &[col, type] : fk_shards) {
    switch (type) {
      case FK_REMOVED:
        stmt.RemoveColumn(col);
        break;
      /*
      case FK_EXTERNAL:
        stmt.MutableColumn(col).RemoveConstraint(
            sqlast::ColumnConstraint::Type::FOREIGN_KEY);
        break;
      */
      default:
        continue;
    }
  }
  return stmt;
}


const sqlast::CreateTable ResolveTableSchema(const SharderState &state, const UnshardedTableName &table_name) {
  // if (state.IsSharded(target_table_name))
  //   return absl::UnimplementedError("Owning tables should not be sharded yet");
  return state.GetSchema(table_name);
}

absl::StatusOr<sql::SqlResult> HandleOwningTable(const sqlast::CreateTable &stmt,
                               const OwningTable &owning_table,
                               Connection *connection,
                               UniqueLock *lock)
                               {
  sql::SqlResult result(true);
  SharderState* state = connection->state->sharder_state();
  const UnshardedTableName &relationship_table_name = stmt.table_name(); 
  const ShardedTableName &target_table_name = owning_table.fk_constraint.foreign_table();
  // If this table owns a different table, we need to shard that table now too.
  // It was probably installed without any sharding before
  if (!state->Exists(target_table_name))
    return absl::UnimplementedError("Owning tables are expected to have been created previously");

  const sqlast::CreateTable &target_table_schema = ResolveTableSchema(*state, target_table_name);
  const ColumnName &my_col_name = owning_table.column.column_name();
  const ColumnName &other_col_name = owning_table.fk_constraint.foreign_column();
  int sharded_by_index = target_table_schema.ColumnIndex(other_col_name);

  LOG(INFO) << "Creating secondary index";
  for (auto & prev_sharding_info : state->GetShardingInformation(relationship_table_name)) {
    if (state->HasIndexFor(relationship_table_name, my_col_name, prev_sharding_info.shard_by)) {
      LOG(INFO) << "Skipping index creation, already exists.";
    } else {
      const std::string &index_name = state->GenerateUniqueIndexName(lock);
      const sqlast::CreateIndex create_index(
        index_name,
        relationship_table_name,
        my_col_name
      );

      auto res = index::CreateIndexStateIsAlreadyLocked(
        create_index,
        connection,
        lock
      );

      if (res.ok()) 
        result.Append(std::move(*res), false);
      else 
        return  res.status();


      LOG(INFO) << "created index " << index_name << " on " << relationship_table_name;
    }
  }

  ShardingInformation sharding_info(
    relationship_table_name /* shard kind */,
    NameShardedTable(target_table_name, my_col_name),
    other_col_name,
    sharded_by_index,
    my_col_name // the 'next column' in this case is our column
  );
  ASSIGN_OR_RETURN(auto & ways_to_shard, IsShardingBySupported(sharding_info, *state, target_table_name));
  for (auto & info : ways_to_shard) {
    const ShardedTableName &sharded_table = info.sharded_table_name;
    ASSIGN_OR_RETURN(
      const ForeignKeyShards fk_shards, 
      ShardForeignKeys(target_table_schema, info, *state)
    );
    state->AddShardedTable(
      target_table_name,
      info,
      UpdateTableSchema(target_table_schema, fk_shards, sharded_table)
    );
    LOG(INFO) << "Added sharded table " << target_table_name << " as " << sharded_table;
  }
  return result;
}


}  // namespace

absl::StatusOr<sql::SqlResult> Shard(const sqlast::CreateTable &stmt,
                                     Connection *connection) {
  connection->perf->Start("Create");
  shards::SharderState *state = connection->state->sharder_state();
  dataflow::DataFlowState *dataflow_state = connection->state->dataflow_state();
  UniqueLock lock = state->WriterLock();

  // extract table_name from the parsed sqlast CreateTable statement
  const std::string &table_name = stmt.table_name();
  // if table already exists, raise error
  if (state->Exists(table_name)) {
    return absl::InvalidArgumentError("Table already exists!");
  }

  // Determine if this table is special: maybe it has PII fields, or maybe it
  // is linked to an existing shard via a foreign key.

  // check if column name starts with PII_
  bool has_pii = HasPII(stmt);
  std::list<ShardingInformation> sharding_information;
  std::optional<OwningTable> owning_table;
  ASSIGN_OR_RETURN(std::tie(sharding_information, owning_table),
                   ShardTable(stmt, *state));

  sql::SqlResult result(true);
  // Sharding scenarios.
  auto &exec = connection->executor;
  if (has_pii && sharding_information.size() == 0) {
    // Case 1: has pii but not linked to shards.
    // This means that this table define a type of user for which shards must be
    // created! Hence, it is a shard kind!
    ASSIGN_OR_RETURN(std::string pk, GetPK(stmt));
    state->AddShardKind(table_name, pk);
    state->AddUnshardedTable(table_name, stmt);
    result = exec.Default(&stmt);

  } else if (!has_pii && sharding_information.size() > 0) {
    for (auto &info_0 : sharding_information) {
      ASSIGN_OR_RETURN(auto & ways_to_shard , IsShardingBySupported(info_0, *state, table_name));
      for (auto & info : ways_to_shard) {
        // Case 2: no pii but is linked to shards.
        // This means that this table should be created inside shards of the kind it
        // is linked to. We must also drop the foreign key column(s) that link it to
        // the shard.
        if (info.shard_kind == table_name) {
          ASSIGN_OR_RETURN(std::string pk, GetPK(stmt));
          state->AddShardKind(table_name, pk);
        }

        // Determine what to do to each column that is a foreign key.
        ASSIGN_OR_RETURN(ForeignKeyShards fk_shards,
                        ShardForeignKeys(stmt, info, *state));
        // Apply sharding and come up with new schema.
        sqlast::CreateTable sharded_stmt =
            UpdateTableSchema(stmt, fk_shards, info.sharded_table_name);
        // Add the sharding information to state.
        state->AddShardedTable(table_name, info, sharded_stmt);
      }
    }

  } else if (!has_pii && sharding_information.size() == 0) {
    // Case 3: neither pii nor linked.
    // The table does not belong to a shard and needs no further modification!
    state->AddUnshardedTable(table_name, stmt);
    result = exec.Default(&stmt);
  } else {
    // Has pii and linked to a shard is a logical schema error.
    return absl::UnimplementedError("Sharded Table cannot have PII fields!");
  }

  // Add table schema and its PK column.
  auto pk_result = GetPK(stmt);
  if (pk_result.ok()) {
    const std::string &pk = pk_result.value();
    state->AddSchema(table_name, stmt, stmt.ColumnIndex(pk), pk);
  } else {
    state->AddSchema(table_name, stmt, -1, "");
  }
  dataflow_state->AddTableSchema(stmt);
  CHECK_STATUS(IndexAccessor(stmt, connection, &lock));


  if (owning_table) {
    if (has_pii)
      return absl::UnimplementedError("'OWNING' tables cannot be data subject tables!");

    if (owning_table->owning_type == OwningT::OWNING) {
      CHECK_STATUS(HandleOwningTable(stmt, *owning_table, connection, &lock));
    } else if (owning_table->owning_type == OwningT::ACCESSING) {
      LOG(INFO) << "Found ACCESSING table";
      const auto anon_cols = GetAnonCols(owning_table->column, stmt.GetColumns());
      CHECK_STATUS(MakeAccessible(connection, owning_table->fk_constraint.foreign_table(), table_name, owning_table->column.column_name(), anon_cols, &lock));
    } else {
      LOG(FATAL) << "Weird `OwningT` value";
    }
  }

  connection->perf->End("Create");
  return result;
}

}  // namespace create
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
