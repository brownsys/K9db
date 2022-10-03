// CREATE TABLE statements sharding and rewriting.
#include "pelton/shards/sqlengine/create.h"

#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sql/abstract_connection.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace create {

namespace {

#define M(it) std::make_move_iterator(it)

struct Annotations {
  std::vector<size_t> accessors;
  std::vector<size_t> explicit_owners;
  std::vector<size_t> implicit_owners;
  std::vector<size_t> owns;
  std::vector<size_t> accesses;
};

absl::StatusOr<Annotations> Discover(const sqlast::CreateTable &stmt,
                                     Connection *connection) {
  SharderState &sstate = connection->state->SharderState();
  // Make sure owners are not ambigious.
  Annotations annotations;
  for (size_t i = 0; i < stmt.GetColumns().size(); i++) {
    const sqlast::ColumnDefinition &col = stmt.GetColumns().at(i);
    if (!col.HasConstraint(sqlast::ColumnConstraint::Type::FOREIGN_KEY)) {
      continue;
    }

    // Make sure all FK point to existing tables.
    const sqlast::ColumnConstraint &fk = col.GetForeignKeyConstraint();
    if (!sstate.TableExists(fk.foreign_table())) {
      return absl::InvalidArgumentError("FK points to nonexisting table");
    }
    const Table &target = sstate.GetTable(fk.foreign_table());
    if (!target.schema.HasColumn(fk.foreign_column())) {
      return absl::InvalidArgumentError("FK points to nonexisting column");
    }

    // Check if this points to the PK.
    size_t index = target.schema.IndexOf(fk.foreign_column());
    bool points_to_pk = target.schema.keys().at(0) == index;

    // Handle various annotations.
    bool foreign_owned = sstate.IsOwned(fk.foreign_table());
    bool foreign_accessed = sstate.IsAccessed(fk.foreign_table());
    if (IsOwner(col)) {
      if (!foreign_owned) {
        return absl::InvalidArgumentError("OWNER to a non data subject");
      }
      if (!points_to_pk) {
        return absl::InvalidArgumentError("OWNER doesn't point to PK");
      }
      annotations.explicit_owners.push_back(i);
    } else if (IsAccessor(col)) {
      if (!foreign_accessed) {
        return absl::InvalidArgumentError("ACCESSOR to a non data subject");
      }
      if (!points_to_pk) {
        return absl::InvalidArgumentError("ACCESSOR doesn't point to PK");
      }
      annotations.accessors.push_back(i);
    } else if (IsOwns(col)) {
      if (!points_to_pk) {
        return absl::InvalidArgumentError("OWNS doesn't point to PK");
      }
      annotations.owns.push_back(i);
    } else if (IsAccesses(col)) {
      if (!points_to_pk) {
        return absl::InvalidArgumentError("ACCESSES doesn't point to PK");
      }
      annotations.accesses.push_back(i);
    } else if (foreign_owned) {
      if (!points_to_pk) {
        return absl::InvalidArgumentError("implicit OWNER doesn't point to PK");
      }
      annotations.implicit_owners.push_back(i);
    }
  }

  // Ambiguity: need to explicitly specify which columns are owners.
  if (annotations.implicit_owners.size() > 1 &&
      annotations.explicit_owners.size() == 0) {
    return absl::InvalidArgumentError("Many potential OWNERs, be explicit");
  }

  return std::move(annotations);
}

absl::StatusOr<IndexDescriptor> MakeIndex(const std::string &indexed_table,
                                          const std::string &indexed_column,
                                          const ShardDescriptor &next,
                                          Connection *connection,
                                          util::UniqueLock *lock) {
  // Create a new unique name for the index.
  uint64_t idx = connection->state->SharderState().IncrementIndexCount();
  std::string index_name = "_index_" + std::to_string(idx);
  // Check the sharding type of next.
  switch (next.type) {
    case InfoType::DIRECT: {
      const DirectInfo &direct = std::get<DirectInfo>(next.info);
      SimpleIndexInfo info = {direct.column};
      IndexDescriptor descriptor = {index_name, indexed_table, indexed_column,
                                    IndexType::SIMPLE, info};
      MOVE_OR_RETURN(sql::SqlResult result,
                     index::CreateIndex(descriptor, connection, lock));
      if (!result.Success()) {
        return absl::InternalError("Cannot create required index");
      }
      return descriptor;
    }
    case InfoType::TRANSITIVE: {
      const TransitiveInfo &transitive = std::get<TransitiveInfo>(next.info);
      const std::string &shard_column =
          std::visit([](const auto &arg) { return arg.shard_column; },
                     transitive.index->info);
      JoinedIndexInfo info = {transitive.index->index_name, transitive.column,
                              transitive.index->indexed_column, shard_column};
      IndexDescriptor descriptor = {index_name, indexed_table, indexed_column,
                                    IndexType::JOINED, info};
      MOVE_OR_RETURN(sql::SqlResult result,
                     index::CreateIndex(descriptor, connection, lock));
      if (!result.Success()) {
        return absl::InternalError("Cannot create required index");
      }
      return descriptor;
    }
    case InfoType::VARIABLE: {
      // Can re-use the variable index as is!
      return std::get<VariableInfo>(next.info).index.value();
    }
  }
  return absl::InternalError("Unreachable code in MakeIndex()!");
}

// origin = the many-2-many table pointing towards the table under consideration
// with an OWNS fk.
// indexed_table is the name of that table, indexed_column is the OWNS fk.
absl::StatusOr<IndexDescriptor> MakeVariableIndex(
    const std::string &indexed_table, const std::string &indexed_column,
    const ShardDescriptor &origin, Connection *connection,
    util::UniqueLock *lock) {
  // Create a new unique name for the index.
  uint64_t idx = connection->state->SharderState().IncrementIndexCount();
  std::string index_name = "_index_" + std::to_string(idx);
  // Check the sharding type of next.
  switch (origin.type) {
    case InfoType::DIRECT: {
      const DirectInfo &direct = std::get<DirectInfo>(origin.info);
      SimpleIndexInfo info = {direct.column};
      IndexDescriptor descriptor = {index_name, indexed_table, indexed_column,
                                    IndexType::SIMPLE, info};
      MOVE_OR_RETURN(sql::SqlResult result,
                     index::CreateIndex(descriptor, connection, lock));
      if (!result.Success()) {
        return absl::InternalError("Cannot create required index");
      }
      return descriptor;
    }
    case InfoType::TRANSITIVE: {
      const TransitiveInfo &transitive = std::get<TransitiveInfo>(origin.info);
      const std::string &shard_column =
          std::visit([](const auto &arg) { return arg.shard_column; },
                     transitive.index->info);
      JoinedIndexInfo info = {transitive.index->index_name, transitive.column,
                              transitive.index->indexed_column, shard_column};
      IndexDescriptor descriptor = {index_name, indexed_table, indexed_column,
                                    IndexType::JOINED, info};
      MOVE_OR_RETURN(sql::SqlResult result,
                     index::CreateIndex(descriptor, connection, lock));
      if (!result.Success()) {
        return absl::InternalError("Cannot create required index");
      }
      return descriptor;
    }
    case InfoType::VARIABLE: {
      // Join with variable index.
      const VariableInfo &variable = std::get<VariableInfo>(origin.info);
      const std::string &shard_column =
          std::visit([](const auto &arg) { return arg.shard_column; },
                     variable.index->info);
      JoinedIndexInfo info = {variable.index->index_name, variable.column,
                              variable.index->indexed_column, shard_column};
      IndexDescriptor descriptor = {index_name, indexed_table, indexed_column,
                                    IndexType::JOINED, info};
      MOVE_OR_RETURN(sql::SqlResult result,
                     index::CreateIndex(descriptor, connection, lock));
      if (!result.Success()) {
        return absl::InternalError("Cannot create required index");
      }
      return descriptor;
    }
  }
  return absl::InternalError("Unreachable code in MakeIndex()!");
}

// Create a shard descriptor for this table corresponding to each shard
// descriptor that current exists for the given foreign table.
std::vector<std::unique_ptr<ShardDescriptor>> MakeDescriptors(
    bool owners, bool create_indices, const sqlast::ColumnDefinition &fk_col,
    size_t fk_column_index, sqlast::ColumnDefinition::Type fk_column_type,
    Connection *connection, util::UniqueLock *lock) {
  SharderState &sstate = connection->state->SharderState();

  // Identify foreign table and column.
  const std::string &fk_colname = fk_col.column_name();
  const sqlast::ColumnConstraint &fk = fk_col.GetForeignKeyConstraint();
  const std::string &next_table = fk.foreign_table();
  const std::string &next_col = fk.foreign_column();
  const Table &tbl = sstate.GetTable(next_table);
  size_t next_col_index = tbl.schema.IndexOf(next_col);
  const std::vector<std::unique_ptr<ShardDescriptor>> &vec =
      owners ? tbl.owners : tbl.accessors;

  // Loop over foreign table descriptors and transform them into descriptors
  // for this table.
  std::vector<std::unique_ptr<ShardDescriptor>> result;
  for (const std::unique_ptr<ShardDescriptor> &next : vec) {
    ShardDescriptor descriptor;
    descriptor.shard_kind = next->shard_kind;
    if (next->shard_kind == next_table) {  // Direct sharding.
      descriptor.type = InfoType::DIRECT;
      descriptor.info = DirectInfo{fk_colname, fk_column_index, fk_column_type};
    } else {  // Transitive sharding.
      descriptor.type = InfoType::TRANSITIVE;
      std::optional<IndexDescriptor> idx;
      if (create_indices) {
        MOVE_OR_PANIC(idx,
                      MakeIndex(next_table, next_col, *next, connection, lock));
      }
      descriptor.info = TransitiveInfo{
          fk_colname, fk_column_index, fk_column_type, next.get(),
          next_table, next_col,        next_col_index, idx};
    }
    result.push_back(std::make_unique<ShardDescriptor>(descriptor));
  }
  return result;
}

// Similar to MakeDescriptors but for opposite direction annotations,
// i.e. OWNS and ACCESSES.
std::vector<std::unique_ptr<ShardDescriptor>> MakeReverseDescriptors(
    bool owners, bool create_indices, const Table &origin,
    const sqlast::ColumnDefinition &origin_col, size_t origin_index,
    Connection *connection, util::UniqueLock *lock) {
  SharderState &sstate = connection->state->SharderState();

  // Find column information about the origin table.
  const std::string &origin_table_name = origin.table_name;
  const std::string &origin_column_name = origin_col.column_name();
  sqlast::ColumnDefinition::Type type = origin.schema.TypeOf(origin_index);
  const std::vector<std::unique_ptr<ShardDescriptor>> &vec =
      owners ? origin.owners : origin.accessors;

  // Find information about the target owned table.
  const sqlast::ColumnConstraint &fk = origin_col.GetForeignKeyConstraint();
  const std::string &target_table_name = fk.foreign_table();
  const std::string &target_column_name = fk.foreign_column();
  const Table &target = sstate.GetTable(target_table_name);
  size_t target_column_index = target.schema.IndexOf(target_column_name);

  // Loop over origin table descriptors and transform them into descriptors
  // for target foreign table.
  std::vector<std::unique_ptr<ShardDescriptor>> result;
  for (const std::unique_ptr<ShardDescriptor> &descriptor : vec) {
    ShardDescriptor next;
    next.shard_kind = descriptor->shard_kind;
    next.type = InfoType::VARIABLE;
    std::optional<IndexDescriptor> index;
    if (create_indices) {
      MOVE_OR_PANIC(index,
                    MakeVariableIndex(origin_table_name, origin_column_name,
                                      *descriptor, connection, lock));
    }
    next.info = VariableInfo{
        target_column_name, target_column_index, type,         descriptor.get(),
        origin_table_name,  origin_column_name,  origin_index, index};
    result.push_back(std::make_unique<ShardDescriptor>(next));
  }
  return result;
}

}  // namespace

// Determine the ways the table is sharded.
absl::StatusOr<sql::SqlResult> Shard(const sqlast::CreateTable &stmt,
                                     Connection *connection,
                                     util::UniqueLock *lock) {
  SharderState &sstate = connection->state->SharderState();
  dataflow::DataFlowState &dstate = connection->state->DataflowState();
  sql::AbstractConnection *db = connection->state->Database();

  // Make sure table does not exist.
  const std::string &table_name = stmt.table_name();
  if (sstate.TableExists(table_name)) {
    return absl::InvalidArgumentError("Table already exists!");
  }

  // Get the schema of this table.
  dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(stmt);

  // Make sure PK is valid.
  if (schema.keys().size() != 1) {
    return absl::InvalidArgumentError("Invalid PK column");
  }
  size_t pk_index = schema.keys().front();
  const std::string &pk_col = stmt.GetColumns().at(pk_index).column_name();

  // Discover annotations and handle any errors.
  MOVE_OR_RETURN(Annotations annotations, Discover(stmt, connection));

  // All errors are handled by this point.
  // This struct will hold all sharding info about the table being created.
  Table table;
  table.table_name = table_name;
  table.schema = schema;

  /* Check to see if this table is a data subject. */
  if (IsADataSubject(stmt)) {
    // We have a new shard kind.
    sstate.AddShardKind(table_name, pk_col, pk_index);

    // This table is also sharded by itself.
    ShardDescriptor descriptor;
    descriptor.shard_kind = table_name;
    descriptor.type = InfoType::DIRECT;
    descriptor.info = DirectInfo{pk_col, pk_index, schema.TypeOf(pk_index)};
    table.owners.push_back(std::make_unique<ShardDescriptor>(descriptor));
  }
  /* End of data subject. */

  /* Check to see if this table has direct or transitive OWNERS. */
  std::vector<size_t> &eowners = annotations.explicit_owners;
  std::vector<size_t> &iowners = annotations.implicit_owners;
  for (size_t idx : eowners.size() > 0 ? eowners : iowners) {
    const sqlast::ColumnDefinition &col = stmt.GetColumns().at(idx);
    sqlast::ColumnDefinition::Type fk_ctype = schema.TypeOf(idx);
    // Transform foreign table descriptors to descriptors of this table.
    auto v = MakeDescriptors(true, true, col, idx, fk_ctype, connection, lock);
    table.owners.insert(table.owners.end(), M(v.begin()), M(v.end()));
    // Access lattice: if U accesses table A, and our table B is owned by A,
    // then U accesses B.
    v = MakeDescriptors(false, false, col, idx, fk_ctype, connection, lock);
    table.accessors.insert(table.accessors.end(), M(v.begin()), M(v.end()));
  }
  /* End of direct and transitive OWNERS. */

  /* Check to see if this table has direct or transitive ACCESSORS. */
  for (size_t idx : annotations.accessors) {
    const sqlast::ColumnDefinition &col = stmt.GetColumns().at(idx);
    sqlast::ColumnDefinition::Type fk_ctype = schema.TypeOf(idx);
    // Transform foreign accessor descriptors to descriptors of this table.
    // Both owners and accessors of foreign table get access of this table.
    auto v = MakeDescriptors(true, false, col, idx, fk_ctype, connection, lock);
    table.accessors.insert(table.accessors.end(), M(v.begin()), M(v.end()));
    v = MakeDescriptors(false, false, col, idx, fk_ctype, connection, lock);
    table.accessors.insert(table.accessors.end(), M(v.begin()), M(v.end()));
  }
  /* End of direct and transitive ACCESSORS. */

  /* Now we create this table and add its information to our state. */
  const Table &table_ref = sstate.AddTable(std::move(table));
  dstate.AddTableSchema(table_name, schema);
  sql::SqlResult result = sql::SqlResult(db->ExecuteCreateTable(stmt));
  /* End of table creation - now we need to make sure any effects this table
     has on previously created table is handled. */

  /* Look for OWNS annotations: target table becomes owned by this table! */
  for (size_t origin_index : annotations.owns) {
    const sqlast::ColumnDefinition &col = stmt.GetColumns().at(origin_index);
    const sqlast::ColumnConstraint &fk = col.GetForeignKeyConstraint();
    const std::string &target_table = fk.foreign_table();
    // Every way of owning this table becomes a way of owning the target table.
    auto v = MakeReverseDescriptors(true, true, table_ref, col, origin_index,
                                    connection, lock);
    CHECK_STATUS(sstate.AddTableOwner(target_table, std::move(v)));
    // Every way of accessing this table becomes a way of accessing target.
    v = MakeReverseDescriptors(false, false, table_ref, col, origin_index,
                               connection, lock);
    CHECK_STATUS(sstate.AddTableAccessor(target_table, std::move(v)));
  }
  /* End of OWNS. */

  /* Look for ACCESSES annotations: target becomes accessible by this table! */
  for (size_t origin_index : annotations.accesses) {
    const sqlast::ColumnDefinition &col = stmt.GetColumns().at(origin_index);
    const sqlast::ColumnConstraint &fk = col.GetForeignKeyConstraint();
    const std::string &target_table = fk.foreign_table();
    // Every way of owning this table becomes a way of accessing the target.
    auto v = MakeReverseDescriptors(true, false, table_ref, col, origin_index,
                                    connection, lock);
    CHECK_STATUS(sstate.AddTableAccessor(target_table, std::move(v)));
    // Every way of accessing this table becomes a way of accessing target.
    v = MakeReverseDescriptors(false, false, table_ref, col, origin_index,
                               connection, lock);
    CHECK_STATUS(sstate.AddTableAccessor(target_table, std::move(v)));
  }
  /* End of ACCESSES. */

  return result;
}

}  // namespace create
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
