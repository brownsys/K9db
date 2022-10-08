// CREATE TABLE statements sharding and rewriting.
#include "pelton/shards/sqlengine/create.h"

#include <iterator>
#include <optional>
#include <utility>

#include "glog/logging.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {

// Helper macro for appending src vector at the end of dst vector by move.
#define APPEND_MOVE(dst, src)                                 \
  dst.insert(dst.end(), std::make_move_iterator(src.begin()), \
             std::make_move_iterator(src.end()))

/*
 * Helpers for finding annotations in the create table statement.
 */
absl::StatusOr<CreateContext::Annotations> CreateContext::DiscoverValidate() {
  // Make sure owners are not ambigious.
  CreateContext::Annotations annotations;
  for (size_t i = 0; i < this->stmt_.GetColumns().size(); i++) {
    const sqlast::ColumnDefinition &col = this->stmt_.GetColumns().at(i);
    if (!col.HasConstraint(sqlast::ColumnConstraint::Type::FOREIGN_KEY)) {
      continue;
    }

    // Make sure all FK point to existing tables.
    const sqlast::ColumnConstraint &fk = col.GetForeignKeyConstraint();
    ASSERT_RET(this->sstate_.TableExists(fk.foreign_table()), InvalidArgument,
               "FK points to nonexisting table");
    const Table &target = this->sstate_.GetTable(fk.foreign_table());
    ASSERT_RET(target.schema.HasColumn(fk.foreign_column()), InvalidArgument,
               "FK points to nonexisting column");

    // Check if this points to the PK.
    size_t index = target.schema.IndexOf(fk.foreign_column());
    bool points_to_pk = target.schema.keys().at(0) == index;

    // Handle various annotations.
    bool foreign_owned = this->sstate_.IsOwned(fk.foreign_table());
    bool foreign_accessed = this->sstate_.IsAccessed(fk.foreign_table());
    if (IsOwner(col)) {
      ASSERT_RET(foreign_owned, InvalidArgument, "OWNER to a non data subject");
      ASSERT_RET(points_to_pk, InvalidArgument, "OWNER doesn't point to PK");
      annotations.explicit_owners.push_back(i);
    } else if (IsAccessor(col)) {
      ASSERT_RET(foreign_accessed, InvalidArgument,
                 "ACCESSOR to a non data subject");
      ASSERT_RET(points_to_pk, InvalidArgument, "ACCESSOR doesn't point to PK");
      annotations.accessors.push_back(i);
    } else if (IsOwns(col)) {
      ASSERT_RET(points_to_pk, InvalidArgument, "OWNS doesn't point to PK");
      annotations.owns.push_back(i);
    } else if (IsAccesses(col)) {
      ASSERT_RET(points_to_pk, InvalidArgument, "ACCESSES doesn't point to PK");
      annotations.accesses.push_back(i);
    } else if (foreign_owned) {
      ASSERT_RET(points_to_pk, InvalidArgument,
                 "implicit OWNER doesn't point to PK");
      annotations.implicit_owners.push_back(i);
    }
  }

  // Ambiguity: need to explicitly specify which columns are owners.
  bool many_impl = annotations.implicit_owners.size() > 1;
  bool has_expl = annotations.explicit_owners.size() > 0;
  ASSERT_RET(has_expl || !many_impl, InvalidArgument,
             "Many potential OWNERs, be explicit");

  return std::move(annotations);
}

/*
 * Helpers for creating indices for transitive and variable ownership.
 */

absl::StatusOr<IndexDescriptor> CreateContext::MakeIndex(
    const std::string &indexed_table, const std::string &indexed_column,
    const ShardDescriptor &next) {
  // Create a new unique name for the index.
  uint64_t idx = this->sstate_.IncrementIndexCount();
  std::string index_name = "_index_" + std::to_string(idx);
  // Check the sharding type of next.
  switch (next.type) {
    case InfoType::DIRECT: {
      const DirectInfo &direct = std::get<DirectInfo>(next.info);
      SimpleIndexInfo info = {direct.column};
      IndexDescriptor descriptor = {index_name, indexed_table, indexed_column,
                                    IndexType::SIMPLE, info};
      MOVE_OR_RETURN(sql::SqlResult result,
                     index::CreateIndex(descriptor, this->conn_, this->lock_));
      ASSERT_RET(result.Success(), Internal, "Error D index");
      return descriptor;
    }
    case InfoType::TRANSITIVE: {
      const TransitiveInfo &transitive = std::get<TransitiveInfo>(next.info);
      const std::string &shard_column =
          EXTRACT_VARIANT(shard_column, transitive.index->info);
      JoinedIndexInfo info = {transitive.index->index_name, transitive.column,
                              transitive.index->indexed_column, shard_column};
      IndexDescriptor descriptor = {index_name, indexed_table, indexed_column,
                                    IndexType::JOINED, info};
      MOVE_OR_RETURN(sql::SqlResult result,
                     index::CreateIndex(descriptor, this->conn_, this->lock_));
      ASSERT_RET(result.Success(), Internal, "Error T index");
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
absl::StatusOr<IndexDescriptor> CreateContext::MakeVIndex(
    const std::string &indexed_table, const std::string &indexed_column,
    const ShardDescriptor &origin) {
  // Create a new unique name for the index.
  uint64_t idx = this->sstate_.IncrementIndexCount();
  std::string index_name = "_index_" + std::to_string(idx);
  // Check the sharding type of next.
  switch (origin.type) {
    case InfoType::DIRECT: {
      const DirectInfo &direct = std::get<DirectInfo>(origin.info);
      SimpleIndexInfo info = {direct.column};
      IndexDescriptor descriptor = {index_name, indexed_table, indexed_column,
                                    IndexType::SIMPLE, info};
      MOVE_OR_RETURN(sql::SqlResult result,
                     index::CreateIndex(descriptor, this->conn_, this->lock_));
      ASSERT_RET(result.Success(), Internal, "Error VD index");
      return descriptor;
    }
    case InfoType::TRANSITIVE: {
      const TransitiveInfo &transitive = std::get<TransitiveInfo>(origin.info);
      const std::string &shard_column =
          EXTRACT_VARIANT(shard_column, transitive.index->info);
      JoinedIndexInfo info = {transitive.index->index_name, transitive.column,
                              transitive.index->indexed_column, shard_column};
      IndexDescriptor descriptor = {index_name, indexed_table, indexed_column,
                                    IndexType::JOINED, info};
      MOVE_OR_RETURN(sql::SqlResult result,
                     index::CreateIndex(descriptor, this->conn_, this->lock_));
      ASSERT_RET(result.Success(), Internal, "Error VT index");
      return descriptor;
    }
    case InfoType::VARIABLE: {
      // Join with variable index.
      const VariableInfo &variable = std::get<VariableInfo>(origin.info);
      const std::string &shard_column =
          EXTRACT_VARIANT(shard_column, variable.index->info);
      JoinedIndexInfo info = {variable.index->index_name, variable.column,
                              variable.index->indexed_column, shard_column};
      IndexDescriptor descriptor = {index_name, indexed_table, indexed_column,
                                    IndexType::JOINED, info};
      MOVE_OR_RETURN(sql::SqlResult result,
                     index::CreateIndex(descriptor, this->conn_, this->lock_));
      ASSERT_RET(result.Success(), Internal, "Error VV index");
      return descriptor;
    }
  }
  return absl::InternalError("Unreachable code in MakeIndex()!");
}

/*
 * Helpers for creating ShardingDescriptor for ownership or accessorships.
 */

// Create a shard descriptor for this table corresponding to each shard
// descriptor that current exists for the given foreign table.
std::vector<std::unique_ptr<ShardDescriptor>> CreateContext::MakeFDescriptors(
    bool owners, bool create_indices, const sqlast::ColumnDefinition &fk_col,
    size_t fk_column_index, sqlast::ColumnDefinition::Type fk_column_type) {
  // Identify foreign table and column.
  const std::string &fk_colname = fk_col.column_name();
  const sqlast::ColumnConstraint &fk = fk_col.GetForeignKeyConstraint();
  const std::string &next_table = fk.foreign_table();
  const std::string &next_col = fk.foreign_column();
  const Table &tbl = this->sstate_.GetTable(next_table);
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
        MOVE_OR_PANIC(idx, this->MakeIndex(next_table, next_col, *next));
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
std::vector<std::unique_ptr<ShardDescriptor>> CreateContext::MakeBDescriptors(
    bool owners, bool create_indices, const Table &origin,
    const sqlast::ColumnDefinition &origin_col, size_t origin_index) {
  // Find column information about the origin table.
  const std::string &origin_table = origin.table_name;
  const std::string &col_name = origin_col.column_name();
  sqlast::ColumnDefinition::Type type = origin.schema.TypeOf(origin_index);
  const std::vector<std::unique_ptr<ShardDescriptor>> &vec =
      owners ? origin.owners : origin.accessors;

  // Find information about the target owned table.
  const sqlast::ColumnConstraint &fk = origin_col.GetForeignKeyConstraint();
  const std::string &target_table_name = fk.foreign_table();
  const std::string &target_column_name = fk.foreign_column();
  const Table &target = this->sstate_.GetTable(target_table_name);
  size_t target_column_index = target.schema.IndexOf(target_column_name);

  // Loop over origin table descriptors and transform them into descriptors
  // for target foreign table.
  std::vector<std::unique_ptr<ShardDescriptor>> result;
  for (const std::unique_ptr<ShardDescriptor> &desc : vec) {
    ShardDescriptor next;
    next.shard_kind = desc->shard_kind;
    next.type = InfoType::VARIABLE;
    std::optional<IndexDescriptor> index;
    if (create_indices) {
      MOVE_OR_PANIC(index, this->MakeVIndex(origin_table, col_name, *desc));
    }
    next.info = VariableInfo{
        target_column_name, target_column_index, type, desc.get(), origin_table,
        col_name,           origin_index,        index};
    result.push_back(std::make_unique<ShardDescriptor>(next));
  }
  return result;
}

/*
 * Handling create: Resolves sharding and creates the physical table.
 */

// Determine the ways the table is sharded.
absl::StatusOr<sql::SqlResult> CreateContext::Exec() {
  // Make sure table does not exist.
  ASSERT_RET(!this->sstate_.TableExists(this->table_name_), InvalidArgument,
             "Table exists!");

  // Get the schema of this table.
  this->schema_ = dataflow::SchemaFactory::Create(this->stmt_);
  this->table_.schema = this->schema_;

  // Make sure PK is valid.
  ASSERT_RET(this->schema_.keys().size() == 1, InvalidArgument, "Invalid PK");
  size_t pk_index = this->schema_.keys().front();
  const std::string &pk_col =
      this->stmt_.GetColumns().at(pk_index).column_name();

  // Discover annotations and handle any errors.
  MOVE_OR_RETURN(Annotations annotations, this->DiscoverValidate());

  // All errors are handled by this point.
  /* Check to see if this table is a data subject. */
  if (IsADataSubject(this->stmt_)) {
    // We have a new shard kind.
    this->sstate_.AddShardKind(this->table_name_, pk_col, pk_index);

    // This table is also sharded by itself.
    ShardDescriptor desc;
    desc.shard_kind = this->table_name_;
    desc.type = InfoType::DIRECT;
    desc.info = DirectInfo{pk_col, pk_index, this->schema_.TypeOf(pk_index)};
    this->table_.owners.push_back(std::make_unique<ShardDescriptor>(desc));
  }
  /* End of data subject. */

  /* Check to see if this table has direct or transitive OWNERS. */
  std::vector<size_t> &eowners = annotations.explicit_owners;
  std::vector<size_t> &iowners = annotations.implicit_owners;
  for (size_t idx : eowners.size() > 0 ? eowners : iowners) {
    const sqlast::ColumnDefinition &col = this->stmt_.GetColumns().at(idx);
    sqlast::ColumnDefinition::Type fk_ctype = this->schema_.TypeOf(idx);
    // Transform foreign table descriptors to descriptors of this table.
    auto v = this->MakeFDescriptors(true, true, col, idx, fk_ctype);
    APPEND_MOVE(this->table_.owners, v);
    // Access lattice: if U accesses table A, and our table B is owned by A,
    // then U accesses B.
    v = this->MakeFDescriptors(false, false, col, idx, fk_ctype);
    APPEND_MOVE(this->table_.accessors, v);
  }
  /* End of direct and transitive OWNERS. */

  /* Check to see if this table has direct or transitive ACCESSORS. */
  for (size_t idx : annotations.accessors) {
    const sqlast::ColumnDefinition &col = this->stmt_.GetColumns().at(idx);
    sqlast::ColumnDefinition::Type fk_ctype = this->schema_.TypeOf(idx);
    // Transform foreign accessor descriptors to descriptors of this table.
    // Both owners and accessors of foreign table get access of this table.
    auto v = this->MakeFDescriptors(true, false, col, idx, fk_ctype);
    APPEND_MOVE(this->table_.accessors, v);
    v = this->MakeFDescriptors(false, false, col, idx, fk_ctype);
    APPEND_MOVE(this->table_.accessors, v);
  }
  /* End of direct and transitive ACCESSORS. */

  /* Now we create this table and add its information to our state. */
  const Table &table_ref = this->sstate_.AddTable(std::move(this->table_));
  this->dstate_.AddTableSchema(this->table_name_, this->schema_);
  sql::SqlResult result(this->db_->ExecuteCreateTable(this->stmt_));
  /* End of table creation - now we need to make sure any effects this table
     has on previously created table is handled. */

  /* Look for OWNS annotations: target table becomes owned by this table! */
  for (size_t idx : annotations.owns) {
    const sqlast::ColumnDefinition &col = this->stmt_.GetColumns().at(idx);
    const sqlast::ColumnConstraint &fk = col.GetForeignKeyConstraint();
    const std::string &target_table = fk.foreign_table();
    // Every way of owning this table becomes a way of owning the target table.
    auto v = this->MakeBDescriptors(true, true, table_ref, col, idx);
    CHECK_STATUS(this->sstate_.AddTableOwner(target_table, std::move(v)));

    // Every way of accessing this table becomes a way of accessing target.
    v = this->MakeBDescriptors(false, false, table_ref, col, idx);
    CHECK_STATUS(this->sstate_.AddTableAccessor(target_table, std::move(v)));
  }
  /* End of OWNS. */

  /* Look for ACCESSES annotations: target becomes accessible by this table! */
  for (size_t idx : annotations.accesses) {
    const sqlast::ColumnDefinition &col = this->stmt_.GetColumns().at(idx);
    const sqlast::ColumnConstraint &fk = col.GetForeignKeyConstraint();
    const std::string &target_table = fk.foreign_table();
    // Every way of owning this table becomes a way of accessing the target.
    auto v = this->MakeBDescriptors(true, false, table_ref, col, idx);
    CHECK_STATUS(this->sstate_.AddTableAccessor(target_table, std::move(v)));
    // Every way of accessing this table becomes a way of accessing target.
    v = this->MakeBDescriptors(false, false, table_ref, col, idx);
    CHECK_STATUS(this->sstate_.AddTableAccessor(target_table, std::move(v)));
  }
  /* End of ACCESSES. */

  return result;
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
