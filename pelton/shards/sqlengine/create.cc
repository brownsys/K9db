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
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

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
absl::StatusOr<std::optional<ShardKind>> ShouldShardBy(
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
absl::Status IsShardingBySupported(ShardingInformation *info,
                                   const SharderState &state) {
  const std::string &foreign_table = info->shard_kind;
  bool foreign_sharded = state.IsSharded(foreign_table);
  bool foreign_pii = state.IsPII(foreign_table);

  if (foreign_pii && foreign_sharded) {
    return absl::InvalidArgumentError("Foreign key into a PII + sharded table");
  } else if (foreign_pii) {
    return absl::OkStatus();
  } else if (foreign_sharded) {
    const auto &info_list = state.GetShardingInformation(foreign_table);
    if (info_list.size() > 1) {
      return absl::InvalidArgumentError("Owning FK into mutli owned table");
    }

    const ShardingInformation &other = info_list.front();
    if (!state.HasIndexFor(foreign_table, info->next_column, other.shard_by)) {
      return absl::InvalidArgumentError(
          "Cannot have a transitive FK pointing to non-index column");
    }

    const FlowName &index =
        state.IndexFlow(foreign_table, info->next_column, other.shard_by);
    if (!info->MakeTransitive(other, index)) {
      return absl::InvalidArgumentError("Transitive FK is too deep");
    }
    return absl::OkStatus();
  }

  return absl::OkStatus();
}

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
absl::StatusOr<std::list<ShardingInformation>> ShardTable(
    const sqlast::CreateTable &stmt, const SharderState &state) {
  // Result is empty by default.
  std::list<ShardingInformation> explicit_owners;
  std::list<ShardingInformation> implicit_owners;
  std::unordered_map<ColumnName, ColumnIndex> index_map;

  // get table name from ast representation of sql statement
  const std::string &table_name = stmt.table_name();
  // Check column definitions for inlined foreign key constraints.
  const auto &columns = stmt.GetColumns();
  for (size_t index = 0; index < columns.size(); index++) {
    // Record index of column for later constraint processing.
    const std::string &column_name = columns[index].column_name();
    // map col name to its index in the table (ascending order)
    index_map.insert({column_name, index});
    // Find foreign key constraint (if exists).
    // check if column starts with OWNER_ (has an owner annotation)
    bool explicit_owner = absl::StartsWith(column_name, "OWNER_");
    // check if column has a foreign key constraint
    const sqlast::ColumnConstraint *fk_constraint = nullptr;
    // loop through all columns, getting their constraints from ast
    for (const auto &constraint : columns[index].GetConstraints()) {
      // if type of constraint is a foreign key, assign value to fk_constraint
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
      ASSIGN_OR_RETURN(std::optional<ShardKind> shard_kind,
                       ShouldShardBy(*fk_constraint, state));
      if (!shard_kind.has_value() && explicit_owner) {
        return absl::InvalidArgumentError("non-sharding fk specified as OWNER");
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
          implicit_owners.emplace_back(shard_kind.value(), sharded_table_name,
                                       column_name, index, foreign_column);
        }
      }
    } else if (explicit_owner) {
      // Not a foreign key but should be sharded explicitly.
      std::string sharded_table_name =
          NameShardedTable(table_name, column_name);
      explicit_owners.emplace_back(table_name, sharded_table_name, column_name,
                                   index, "");
    }
  }

  // If sharding is explicitly specified via OWNER, we are done.
  if (explicit_owners.size() > 0) {
    return explicit_owners;
  }

  // Sharding is implicitly deduced, only works if there is one candidate!
  if (implicit_owners.size() > 1) {
    return absl::InvalidArgumentError(
        "Table with several foreign keys to shards or PII!");
  }
  return implicit_owners;
}

void IndexAccessor(const sqlast::CreateTable &stmt, SharderState &state,
                   dataflow::DataFlowState &dataflow_state) {
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
    bool explicit_accessor = absl::StartsWith(column_name, "ACCESSOR_");

    std::cout << "Column name: " << column_name << std::endl;
    std::cout << "ACCESSOR BOOL: " << explicit_accessor << std::endl;

    if (explicit_accessor) {
      const sqlast::ColumnConstraint *fk_constraint = nullptr;
      for (const auto &constraint : columns[index].GetConstraints()) {
        // if type of constraint is a foreign key, assign value to fk_constraint
        if (constraint.type() == sqlast::ColumnConstraint::Type::FOREIGN_KEY) {
          fk_constraint = &constraint;
          break;
        }
      }
      std::string shard_string = "";
      if (fk_constraint != nullptr) {
        absl::StatusOr<std::optional<ShardKind>> shard_kind =
            ShouldShardBy(*fk_constraint, state);
        shard_string = shard_kind->value();
        std::cout << "STATUS: " << shard_kind->value() << std::endl;
      }
      std::cout << "We have found an accessor: " << column_name << std::endl;
      // CreateIndex(access_column (doctor) is the key, owner_by_value
      // (patient), index is not unique)

      // TODO: known issue: index name is too long, throwing sqlexception
      // terminate called after throwing an instance of
      // 'sql::SQLSyntaxErrorException' what():  (conn=90) Identifier name
      // 'pd3d9446802a44259755d38e6_r_doctors_chat_ACCESSOR_doctor_id_OWNER_patient_id'
      // is too long (when including column name)
      // TODO: fix by replacing string concatenation with field in SharderState containing
      // struct with all fields we need to access in gdpr.cc when selecting accessor data
      sqlast::CreateIndex create_index_stmt{
          "r_" + shard_string + "_" + table_name, table_name, column_name};
      std::cout << "Starting CreateIndex: " << std::endl;
      pelton::shards::sqlengine::index::CreateIndex(create_index_stmt, &state,
                                                    &dataflow_state);

      // Index of shard_id (patient) to Primary Key of chat table, in that row
      // we set the doctor_id col to NULL. Anonymize access annotation. So like
      // if doctor (accessor) deletes his data, his
      // ==> can have "on delete retain" or "on delete anonymise"
    }
  }
  std::cout << "finished!" << std::endl;
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
    } else if (absl::StartsWith(colname, "OWNER_") &&
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
      case FK_EXTERNAL:
        stmt.MutableColumn(col).RemoveConstraint(
            sqlast::ColumnConstraint::Type::FOREIGN_KEY);
        break;
      default:
        continue;
    }
  }
  return stmt;
}

}  // namespace

absl::StatusOr<sql::SqlResult> Shard(const sqlast::CreateTable &stmt,
                                     SharderState *state,
                                     dataflow::DataFlowState *dataflow_state) {
  perf::Start("Create");

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
  // assign return value of ShardTable(..) to sharding_information
  ASSIGN_OR_RETURN(std::list<ShardingInformation> sharding_information,
                   ShardTable(stmt, *state));
  for (auto &info : sharding_information) {
    CHECK_STATUS(IsShardingBySupported(&info, *state));
  }

  sql::SqlResult result = sql::SqlResult(false);
  // Sharding scenarios.
  auto &exec = state->executor();
  if (has_pii && sharding_information.size() == 0) {
    // Case 1: has pii but not linked to shards.
    // This means that this table define a type of user for which shards must be
    // created! Hence, it is a shard kind!
    ASSIGN_OR_RETURN(std::string pk, GetPK(stmt));
    state->AddShardKind(table_name, pk);
    state->AddUnshardedTable(table_name, stmt);
    result = exec.ExecuteDefault(&stmt);

  } else if (!has_pii && sharding_information.size() > 0) {
    // Case 2: no pii but is linked to shards.
    // This means that this table should be created inside shards of the kind it
    // is linked to. We must also drop the foreign key column(s) that link it to
    // the shard.
    for (const auto &info : sharding_information) {
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
    result = sql::SqlResult(true);

  } else if (!has_pii && sharding_information.size() == 0) {
    // Case 3: neither pii nor linked.
    // The table does not belong to a shard and needs no further modification!
    state->AddUnshardedTable(table_name, stmt);
    result = exec.ExecuteDefault(&stmt);
  } else {
    // Has pii and linked to a shard is a logical schema error.
    return absl::UnimplementedError("Sharded Table cannot have PII fields!");
  }

  state->AddSchema(table_name, stmt);
  dataflow_state->AddTableSchema(stmt);
  IndexAccessor(stmt, *state, *dataflow_state);

  perf::End("Create");
  return result;
}

}  // namespace create
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
