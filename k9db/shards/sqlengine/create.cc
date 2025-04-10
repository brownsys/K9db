// CREATE TABLE statements sharding and rewriting.
#include "k9db/shards/sqlengine/create.h"

#include <iterator>
#include <optional>
#include <unordered_set>

#include "glog/logging.h"
#include "k9db/shards/sqlengine/index.h"
#include "k9db/util/status.h"

namespace k9db {
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
    const auto &[foreign_table, foreign_column, fk_type] = fk.ForeignKey();

    // In case this is a self-referring FK.
    bool self_fk = foreign_table == this->stmt_.table_name();
    if (!self_fk) {
      ASSERT_RET(this->sstate_.TableExists(foreign_table), InvalidArgument,
                 "FK points to nonexisting table");
    }

    const Table &target =
        self_fk ? this->table_ : this->sstate_.GetTable(foreign_table);
    ASSERT_RET(target.schema.HasColumn(foreign_column), InvalidArgument,
               "FK points to nonexisting column");

    // Check if this points to the PK, handle various annotations.
    bool foreign_owned, foreign_accessed, points_to_pk;
    if (self_fk) {
      foreign_owned = !annotations.explicit_owners.empty() ||
                      !annotations.implicit_owners.empty() ||
                      this->stmt_.IsDataSubject();
      foreign_accessed = !annotations.accessors.empty() || foreign_owned;
      points_to_pk =
          this->stmt_.GetColumn(foreign_column)
              .HasConstraint(sqlast::ColumnConstraint::Type::PRIMARY_KEY);
    } else {
      foreign_owned = this->sstate_.IsOwned(foreign_table);
      foreign_accessed = this->sstate_.IsAccessed(foreign_table);
      points_to_pk =
          target.schema.keys().at(0) == target.schema.IndexOf(foreign_column);
    }

    if (fk_type == sqlast::ColumnConstraint::FKType::OWNED_BY) {
      ASSERT_RET(foreign_owned, InvalidArgument, "OWNER to a non data subject");
      ASSERT_RET(points_to_pk, InvalidArgument, "OWNER doesn't point to PK");
      ASSERT_RET(!self_fk, InvalidArgument, "OWNER on a self-referencing FK");
      annotations.explicit_owners.push_back(i);
    } else if (fk_type == sqlast::ColumnConstraint::FKType::ACCESSED_BY) {
      ASSERT_RET(foreign_accessed, InvalidArgument,
                 "ACCESSOR to a non data subject");
      ASSERT_RET(points_to_pk, InvalidArgument, "ACCESSOR doesn't point to PK");
      annotations.accessors.push_back(i);
    } else if (fk_type == sqlast::ColumnConstraint::FKType::OWNS) {
      ASSERT_RET(points_to_pk, InvalidArgument, "OWNS doesn't point to PK");
      ASSERT_RET(!self_fk, InvalidArgument, "OWNS on a self-referencing FK");
      annotations.owns.push_back(i);
    } else if (fk_type == sqlast::ColumnConstraint::FKType::ACCESSES) {
      ASSERT_RET(points_to_pk, InvalidArgument, "ACCESSES doesn't point to PK");
      annotations.accesses.push_back(i);
    } else if (foreign_owned) {
      if (fk_type == sqlast::ColumnConstraint::FKType::AUTO) {
        ASSERT_RET(points_to_pk, InvalidArgument,
                   "implicit OWNER doesn't point to PK");
        annotations.implicit_owners.push_back(i);
      }
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
 * Helpers for creating ShardingDescriptor for ownership or accessorships.
 */

// This creates shard descriptor(s) for this table corresponding to the
// given foreign key.
// This looks at the foreign table, and identifies which shard kinds it belongs
// to, and returns sharding desciptors based on those.
// This returns a single shard descriptor for each shard kind the foreign table
// belongs to. Even when the foreign table is owned by a shard kind in different
// ways (i.e. it has two OWNER columns pointing to the same shard kind), we will
// only return a single shard descriptor encapsulating all these ways, and use
// an index when neceassry to abstract them.
std::vector<std::unique_ptr<ShardDescriptor>> CreateContext::MakeFDescriptors(
    bool owners, bool create_indices, const sqlast::ColumnDefinition &fk_col,
    size_t fk_column_index, sqlast::ColumnDefinition::Type fk_column_type) {
  // Identify foreign table and column.
  const std::string &fk_colname = fk_col.column_name();
  const sqlast::ColumnConstraint &fk = fk_col.GetForeignKeyConstraint();
  const auto &[next_table, next_col, _] = fk.ForeignKey();

  Table &tbl = next_table == this->table_name_
                   ? this->table_
                   : this->sstate_.GetTable(next_table);

  size_t next_col_index = tbl.schema.IndexOf(next_col);
  const std::vector<std::unique_ptr<ShardDescriptor>> &vec =
      owners ? tbl.owners : tbl.accessors;

  // Loop over foreign table descriptors and transform them into descriptors
  // for this table.
  std::unordered_set<std::string> shard_kinds;
  std::vector<std::unique_ptr<ShardDescriptor>> result;
  for (const std::unique_ptr<ShardDescriptor> &next : vec) {
    const std::string &shard_kind = next->shard_kind;
    if (!shard_kinds.insert(shard_kind).second) {
      continue;
    }

    // First time we see this shard_kind.
    ShardDescriptor descriptor;
    descriptor.shard_kind = shard_kind;
    if (shard_kind == next_table && next_table != this->table_name_) {
      // Direct sharding.
      descriptor.type = InfoType::DIRECT;
      descriptor.info = DirectInfo{fk_colname, fk_column_index, fk_column_type,
                                   next_col, next_col_index};
    } else {
      // Transitive sharding.
      descriptor.type = InfoType::TRANSITIVE;
      IndexDescriptor *index = nullptr;
      if (create_indices) {
        // We need an index for next_table[next_col] and shard_kind.
        auto &col_indices = tbl.indices[next_col];
        if (col_indices.count(shard_kind) == 0) {
          MOVE_OR_PANIC(IndexDescriptor tmp,
                        index::Create(next_table, shard_kind, next_col,
                                      this->conn_, this->lock_));
          col_indices.emplace(
              shard_kind, std::make_unique<IndexDescriptor>(std::move(tmp)));
        }
        index = col_indices.at(shard_kind).get();
      }
      descriptor.info = TransitiveInfo{
          fk_colname, fk_column_index, fk_column_type, next_table,
          next_col,   next_col_index,  index};
    }
    result.push_back(std::make_unique<ShardDescriptor>(descriptor));
  }
  return result;
}

// Similar to MakeDescriptors but for opposite direction annotations,
// i.e. OWNS and ACCESSES.
std::vector<std::unique_ptr<ShardDescriptor>> CreateContext::MakeBDescriptors(
    bool owners, bool create_indices, Table *origin,
    const sqlast::ColumnDefinition &origin_col, size_t origin_index) {
  // Find column information about the origin table.
  const std::string &origin_table = origin->table_name;
  const std::string &col_name = origin_col.column_name();
  sqlast::ColumnDefinition::Type type = origin->schema.TypeOf(origin_index);
  const std::vector<std::unique_ptr<ShardDescriptor>> &vec =
      owners ? origin->owners : origin->accessors;

  // Find information about the target owned table.
  const sqlast::ColumnConstraint &fk = origin_col.GetForeignKeyConstraint();
  const auto &[target_table_name, target_column_name, _] = fk.ForeignKey();
  const Table &target = this->sstate_.GetTable(target_table_name);
  size_t target_column_index = target.schema.IndexOf(target_column_name);

  // Loop over origin table descriptors and transform them into descriptors
  // for target foreign table.
  std::unordered_set<std::string> shard_kinds;
  std::vector<std::unique_ptr<ShardDescriptor>> result;
  for (const std::unique_ptr<ShardDescriptor> &desc : vec) {
    const std::string &shard_kind = desc->shard_kind;
    if (!shard_kinds.insert(shard_kind).second) {
      continue;
    }

    ShardDescriptor next;
    next.shard_kind = shard_kind;
    next.type = InfoType::VARIABLE;
    next.info = VariableInfo{
        target_column_name, target_column_index, type, origin_table,
        col_name,           origin_index};
    result.push_back(std::make_unique<ShardDescriptor>(next));
  }
  return result;
}

/*
 * Validate anonymization rules.
 */
absl::Status CreateContext::ValidateAnonymizationRules(
    const Annotations &annotations) const {
  // Find PK.
  size_t pk = this->schema_.keys().front();
  const std::string &pk_col = this->schema_.NameOf(pk);

  // Find all OWNED_BY columns.
  std::unordered_set<std::string> owners;
  for (const std::unique_ptr<ShardDescriptor> &desc : this->table_.owners) {
    owners.insert(EXTRACT_VARIANT(column, desc->info));
  }
  for (size_t owns : annotations.owns) {
    owners.insert(this->schema_.NameOf(owns));
  }

  // Loop over anonymization rules.
  const std::vector<sqlast::AnonymizationRule> &rules =
      this->stmt_.GetAnonymizationRules();
  for (size_t i = 0; i < rules.size(); i++) {
    const sqlast::AnonymizationRule &rule = rules.at(i);
    if (rule.GetType() != sqlast::AnonymizationRule::Type::DEL) {
      continue;
    }
    for (const std::string &column : rule.GetAnonymizeColumns()) {
      // Should not anonymize PK.
      ASSERT_RET(pk_col != column, Internal,
                 "DEL Anonymization rule anonymizes PK!");

      // Should not anonymize other OWNED_BY.
      for (const std::string &fk : owners) {
        if (fk == rule.GetDataSubject()) {
          continue;
        }
        ASSERT_RET(fk != column, Internal,
                   "DEL Anonymization rule " + std::to_string(i) +
                       " anonymizes different ownership fk " + fk);
      }
    }
  }

  return absl::OkStatus();
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

  // Track any AUTO_INCREMENT and DEFAULT columns.
  for (const sqlast::ColumnDefinition &column : this->stmt_.GetColumns()) {
    if (column.HasConstraint(sqlast::ColumnConstraint::Type::AUTO_INCREMENT)) {
      size_t idx = this->schema_.IndexOf(column.column_name());
      ASSERT_RET(
          this->schema_.TypeOf(idx) == sqlast::ColumnDefinition::Type::INT,
          InvalidArgument, "AUTO_INCREMENT column must be INT");
      ASSERT_RET(this->table_.defaults.count(column.column_name()) == 0,
                 InvalidArgument, "AUTO_INCREMENT column cannot have default");
      this->table_.auto_increments.insert(column.column_name());
    }
    if (column.HasConstraint(sqlast::ColumnConstraint::Type::DEFAULT)) {
      const sqlast::ColumnConstraint &constraint =
          column.GetConstraintOfType(sqlast::ColumnConstraint::Type::DEFAULT);
      size_t idx = this->schema_.IndexOf(column.column_name());
      ASSERT_RET(constraint.Default().TypeCompatible(this->schema_.TypeOf(idx)),
                 InvalidArgument, "DEFAULT value incompatible with column");
      ASSERT_RET(this->table_.auto_increments.count(column.column_name()) == 0,
                 InvalidArgument, "AUTO_INCREMENT column cannot have default");
      this->table_.defaults.emplace(column.column_name(), constraint.Default());
    }
  }

  // Discover annotations and handle any errors.
  MOVE_OR_RETURN(Annotations annotations, this->DiscoverValidate());

  // All errors are handled by this point.
  /* Check to see if this table is a data subject. */
  if (this->stmt_.IsDataSubject()) {
    // We have a new shard kind.
    this->sstate_.AddShardKind(this->table_name_, pk_col, pk_index);

    // This table is also sharded by itself.
    ShardDescriptor desc;
    desc.shard_kind = this->table_name_;
    desc.type = InfoType::DIRECT;
    desc.info = DirectInfo{pk_col, pk_index, this->schema_.TypeOf(pk_index),
                           pk_col, pk_index};
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

  CHECK_STATUS(this->ValidateAnonymizationRules(annotations));

  /* Now we create this table and add its information to our state. */
  Table *table_ptr = this->sstate_.AddTable(std::move(this->table_));
  this->dstate_.AddTableSchema(this->table_name_, this->schema_);
  sql::SqlResult result(this->db_->ExecuteCreateTable(this->stmt_));
  /* End of table creation - now we need to make sure any effects this table
     has on previously created table is handled. */

  /* Look for OWNS annotations: target table becomes owned by this table! */
  for (size_t idx : annotations.owns) {
    const sqlast::ColumnDefinition &col = this->stmt_.GetColumns().at(idx);
    const auto &[fk_table, _, __] = col.GetForeignKeyConstraint().ForeignKey();
    // Every way of owning this table becomes a way of owning the target table.
    auto v = this->MakeBDescriptors(true, true, table_ptr, col, idx);
    CHECK_STATUS(this->sstate_.AddTableOwner(fk_table, std::move(v)));

    // Every way of accessing this table becomes a way of accessing target.
    v = this->MakeBDescriptors(false, false, table_ptr, col, idx);
    CHECK_STATUS(this->sstate_.AddTableAccessor(fk_table, std::move(v)));
  }
  /* End of OWNS. */

  /* Look for ACCESSES annotations: target becomes accessible by this table! */
  for (size_t idx : annotations.accesses) {
    const sqlast::ColumnDefinition &col = this->stmt_.GetColumns().at(idx);
    const auto &[fk_table, _, __] = col.GetForeignKeyConstraint().ForeignKey();
    // Every way of owning this table becomes a way of accessing the target.
    auto v = this->MakeBDescriptors(true, false, table_ptr, col, idx);
    CHECK_STATUS(this->sstate_.AddTableAccessor(fk_table, std::move(v)));
    // Every way of accessing this table becomes a way of accessing target.
    v = this->MakeBDescriptors(false, false, table_ptr, col, idx);
    CHECK_STATUS(this->sstate_.AddTableAccessor(fk_table, std::move(v)));
  }
  /* End of ACCESSES. */

  /* Reload the value of the atomic counter (for auto increment) in case
     this is a recreation of an existing table after restart. */
  if (table_ptr->auto_increments.size() > 0) {
    const std::string &column = *table_ptr->auto_increments.begin();
    int64_t max = this->db_->GetMaximumValue(this->table_name_, column);
    table_ptr->counter = std::make_unique<std::atomic<int64_t>>(max + 1);
  }

  return result;
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
