// UPDATE statements sharding and rewriting.
#include "k9db/shards/sqlengine/update.h"

#include <cassert>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "k9db/shards/sqlengine/util.h"
#include "k9db/util/iterator.h"
#include "k9db/util/status.h"

#define ACCUM(i, accum)      \
  if (i < 0) {               \
    accum = i;               \
    return absl::OkStatus(); \
  }                          \
  accum += i

namespace k9db {
namespace shards {
namespace sqlengine {

/*
 * Locate the shards that this new record should be inserted to by looking at
 * parent tables.
 */
absl::StatusOr<std::unordered_set<util::ShardName>>
UpdateContext::LocateNewShards(const dataflow::Record &record) const {
  std::unordered_set<util::ShardName> shards;
  // Need to look at each way of sharding this table.
  for (const std::unique_ptr<ShardDescriptor> &desc : this->table_.owners) {
    // Identify value of the column along which we are sharding.
    size_t colidx = EXTRACT_VARIANT(column_index, desc->info);

    const std::string &colname = EXTRACT_VARIANT(column, desc->info);
    sqlast::Value val = record.GetValue(colidx);
    if (val.IsNull()) {
      continue;
    }

    // Handle according to sharding type.
    switch (desc->type) {
      case InfoType::DIRECT: {
        // UserID is the direct fk value.
        const DirectInfo &info = std::get<DirectInfo>(desc->info);
        if (desc->shard_kind != this->table_name_) {
          // Ensure new FK value respects integrity.
          const Table &next_table = this->sstate_.GetTable(desc->shard_kind);
          ASSERT_RET(info.next_column_index == next_table.schema.keys().at(0),
                     Internal, "Direct OWNED_BY FK points to nonPK");
          if (!this->db_->Exists(desc->shard_kind, val)) {
            return absl::InvalidArgumentError("Integrity error: " + colname);
          }
        }
        shards.emplace(desc->shard_kind, val.AsUnquotedString());
        break;
      }
      case InfoType::TRANSITIVE: {
        const TransitiveInfo &info = std::get<TransitiveInfo>(desc->info);

        // Need to ensure that the FK points to an existing record (and lock).
        const Table &next_table = this->sstate_.GetTable(info.next_table);
        ASSERT_RET(info.next_column_index == next_table.schema.keys().at(0),
                   Internal, "Transitive OWNED_BY FK points to nonPK");
        if (!this->db_->Exists(info.next_table, val)) {
          return absl::InvalidArgumentError("Integrity error: " + colname);
        }

        // Locate shards.
        std::unordered_set<util::ShardName> parent_shards =
            this->db_->FindShards(info.next_table, info.next_column_index, val);
        for (const util::ShardName &shard : parent_shards) {
          shards.insert(shard.Copy());
        }
        break;
      }
      case InfoType::VARIABLE: {
        // Integrity is not an issue with respect to the new value as this FK
        // points the other way.
        // It could be an issue for the old value, but since OWNS only points to
        // PK, and we do not support updating PK, it is not an issue!
        const VariableInfo &info = std::get<VariableInfo>(desc->info);

        // OWNS must point to this table's PK.
        const std::string &origin_table = info.origin_relation;
        size_t origin_column = info.origin_column_index;
        ASSERT_RET(colidx == this->schema_.keys().at(0), Internal,
                   "Variable OWNS FK points to nonPK");

        // Locate shards.
        std::unordered_set<util::ShardName> parent_shards =
            this->db_->FindShards(origin_table, origin_column, val);
        for (const util::ShardName &shard : parent_shards) {
          shards.insert(shard.Copy());
        }
        break;
      }
      default:
        return absl::InternalError("Unreachable sharding case");
    }
  }

  // Default shard if no shard exists.
  if (shards.size() == 0) {
    shards.emplace(DEFAULT_SHARD, DEFAULT_SHARD);
  }

  return shards;
}

/*
 * Update records using the given update statement in memory.
 */
absl::StatusOr<std::vector<sqlast::Insert>> UpdateContext::UpdateRecords(
    std::vector<UpdateInfo> *infos) const {
  // Turn SQL statement to a map of updates.
  dataflow::Record::UpdateMap updates;
  const std::vector<std::string> &cols = this->stmt_.GetColumns();
  const auto &vals = this->stmt_.GetValues();
  for (const auto &[col, v] : util::Zip(&cols, &vals)) {
    updates.emplace(col, v.get());
  }

  // Update one record at a time.
  std::vector<sqlast::Insert> result;
  result.reserve(infos->size());
  for (UpdateInfo &info : *infos) {
    // Create updated record.
    info.updated = info.old.Update(updates);

    // Find its new shards and guarantee that the new record respects integrity.
    ASSIGN_OR_RETURN(std::unordered_set<util::ShardName> & set,
                     this->LocateNewShards(info.updated));
    info.new_shards = std::move(set);

    /*
    // Ensure OWNS from parent table are still respected.
    // Need to look at each way of sharding this table.
    // This is not needed because we do not support updating PK columns.
    for (const std::unique_ptr<ShardDescriptor> &desc : this->table_.owners) {
      // Identify value of the column along which we are sharding.
      size_t colidx = EXTRACT_VARIANT(column_index, desc->info);

      const std::string &colname = EXTRACT_VARIANT(column, desc->info);
      sqlast::Value val = record.GetValue(colidx);
      ASSERT_RET(!val.IsNull(), Internal, "OWNER cannot be NULL");

      // Handle according to sharding type.
      switch (desc->type) {
        case InfoType::VARIABLE: {
          // Integrity is not an issue with respect to the new value as this FK
          // points the other way.
          // But it is an issue for the old value.
          const VariableInfo &info = std::get<VariableInfo>(desc->info);

          // Find the parent table and column.
          const std::string &colname = info.column;
          const std::string &parent = info.origin_relation;
          size_t parent_column = info.origin_column_index;

          // Ensure no parents point at the old value.
          sqlast::Value value = info.old.GetValue(info.column_index);
          if (!this->db_->Exists(parent, parent_column, value) {
            return absl::InvalidArgumentError("Integrity error: " + colname);
          }

          break;
        }
        default:
          return absl::InternalError("Unreachable sharding case");
      }
    }
    */

    // Also create it as an INSERT statement.
    std::vector<sqlast::Value> values;
    values.reserve(this->schema_.size());
    for (size_t i = 0; i < this->schema_.size(); i++) {
      values.push_back(info.updated.GetValue(i));
    }
    result.emplace_back(this->table_name_);
    result.back().SetValues(std::move(values));
  }

  return result;
}

/*
 * Returns true if the update statement may change the sharding/ownership of
 * any affected record in the table.
 * False guarantees the update is a no-op wrt to who owns any affected records.
 */
absl::StatusOr<bool> UpdateContext::ModifiesShardingBaseTable() {
  // Ensure all columns being updated exist.
  for (const std::string &column : this->stmt_.GetColumns()) {
    ASSERT_RET(this->schema_.HasColumn(column), Internal,
               "Column does not exist: " + column);
    size_t colidx = this->schema_.IndexOf(column);
    this->update_columns_.insert(colidx);
    // Currently, we do not support updating PK columns.
    for (size_t i : this->schema_.keys()) {  // PK columns
      ASSERT_RET(colidx != i, Internal, "Updating PK column is not supported!");
    }
  }

  // Check if any OWNED_BY or OWNS column are modified by this statement.
  for (const std::unique_ptr<ShardDescriptor> &desc : this->table_.owners) {
    size_t colidx = EXTRACT_VARIANT(column_index, desc->info);
    if (this->update_columns_.count(colidx) > 0) {
      return true;
    }
  }
  return false;
}

/*
 * Same but for dependent tables.
 */
bool UpdateContext::ModifiesShardingDependentTables() {
  for (const auto &[next_table, desc] : this->table_.dependents) {
    size_t colidx = desc->upcolumn_index();
    if (this->update_columns_.count(colidx) > 0) {
      return true;
    }
  }
  return false;
}

/*
 * Executes the update by issuing a delete followed by an insert.
 */
absl::StatusOr<std::vector<UpdateInfo>> UpdateContext::DeleteInsert() {
  LOG(WARNING) << "SLOW UPDATE " << this->stmt_;

  // Delete all the records being updated.
  sql::SqlDeleteSet del = this->db_->ExecuteDelete(this->stmt_.DeleteDomain());
  this->status_ = del.Count();

  // Associate each (deduplicated) record with its old shard.
  std::vector<UpdateInfo> updates;
  std::unordered_map<size_t, size_t> index_to_update;
  for (const auto &[shard, records] : del.Map()) {
    for (size_t idx : records) {
      const dataflow::Record &old = del.Rows().at(idx);
      auto it = index_to_update.find(idx);
      if (it != index_to_update.end()) {
        UpdateInfo &info = updates.at(it->second);
        info.old_shards.insert(shard.Copy());
      } else {
        updates.emplace_back(old.Copy(), old.Copy());
        index_to_update.emplace(idx, updates.size() - 1);
        UpdateInfo &info = updates.back();
        info.old_shards.insert(shard.Copy());
      }
    }
  }

  // Perform update over deleted records in memory.
  MOVE_OR_RETURN(std::vector<sqlast::Insert> inserts,
                 this->UpdateRecords(&updates));

  // Insert updated records!
  for (size_t i = 0; i < inserts.size(); i++) {
    const sqlast::Insert &insert = inserts.at(i);
    for (const util::ShardName &shard : updates.at(i).new_shards) {
      int status = this->db_->ExecuteInsert(insert, shard);
      if (status < 0) {
        return std::vector<UpdateInfo>{};
      }
      this->status_ += status;
    }

    this->positives_.push_back(updates.at(i).updated.Copy());
  }

  // Store the deduplicated list of old and new records.
  this->negatives_ = del.Vec();

  return updates;
}

/*
 * Executes the update directly against the database by overriding data
 * in the database.
 */
std::vector<UpdateInfo> UpdateContext::DirectUpdate() {
  std::vector<UpdateInfo> updates;
  std::unordered_map<size_t, size_t> index_to_update;

  // Associate each (deduplicated) record with its old and new set of shards.
  sql::SqlUpdateSet update_set = this->db_->ExecuteUpdate(this->stmt_);
  this->status_ = update_set.Count();
  for (const auto &[shard, records] : update_set.Map()) {
    for (size_t idx : records) {
      const dataflow::Record &old = update_set.Rows().at(idx);
      const dataflow::Record &updated = update_set.Rows().at(idx + 1);
      auto it = index_to_update.find(idx);
      if (it != index_to_update.end()) {
        UpdateInfo &info = updates.at(it->second);
        info.old_shards.insert(shard.Copy());
        info.new_shards.insert(shard.Copy());
      } else {
        updates.emplace_back(old.Copy(), updated.Copy());
        index_to_update.emplace(idx, updates.size() - 1);
        UpdateInfo &info = updates.back();
        info.old_shards.insert(shard.Copy());
        info.new_shards.insert(shard.Copy());
      }
    }
  }

  // Store the deduplicated list of old and new records.
  std::vector<dataflow::Record> vec = update_set.Vec();
  for (dataflow::Record &record : vec) {
    if (record.IsPositive()) {
      this->positives_.push_back(std::move(record));
    } else {
      this->negatives_.push_back(std::move(record));
    }
  }
  return updates;
}

/*
 * Cascade shard changes to dependent tables.
 */
absl::Status UpdateContext::CascadeDependents(
    const std::vector<UpdateInfo> &cascades) {
  // There are two non-exclusive reasons we may need to CASCADE:
  // (1) The update changes the shards of the records in the update database,
  //     thus dependent records (of any kind) may need to change shards as well.
  // (2) The update changes a column that a dependent table depends on for
  //     ownership. This may occur even if the shards of the current records are
  //     unchanged.
  // Because OWNED_BY FKs can only point to PK, and we do not support updating
  // PK, we do not need to apply (2) for DIRECT or TRANSITIVE dependencies.
  // Instead, we only need to apply (2) for VARIABLE when the OWNS column
  // change.
  // We apply cascades that assign (potentially) new shards to records first,
  // then apply cascades that (potentially) remove records from shards second.
  // This ensures that we only write records to the default shard when they are
  // truely orphaned, as opposed to temporarily as the UPDATE is processed.
  Cascader cascader(this->conn_, this->lock_);

  // Apply to new shards first.
  for (const auto &[next_table, desc] : this->table_.dependents) {
    const std::string &shard_kind = desc->shard_kind;
    ASSERT_RET(shard_kind != this->table_name_, Internal, "self dependency!");

    // Find out if the shards of the updated records were changed.
    // This indiates whether (1) applies or not.
    for (const UpdateInfo &info : cascades) {
      std::unordered_set<util::ShardName> added_shards;
      for (const util::ShardName &shard : info.new_shards) {
        if (shard.ShardKind() == shard_kind) {
          if (info.old_shards.count(shard) == 0) {
            added_shards.insert(shard.Copy());
          }
        }
      }

      // Cascade according to dependency type.
      switch (desc->type) {
        // Only (1) applies to DIRECT and TRANSITIVE.
        case InfoType::DIRECT:
        case InfoType::TRANSITIVE: {
          // Get the corresponding FK columns in this table and dependent table.
          size_t colidx = desc->upcolumn_index();
          ASSERT_RET(colidx == this->schema_.keys().at(0), Internal,
                     "OWNED_BY FK points to nonPK");
          ASSERT_RET(info.old.GetValue(colidx) == info.updated.GetValue(colidx),
                     Internal, "PK value somehow updated!");

          Cascader::Condition condition;
          condition.column = EXTRACT_VARIANT(column_index, desc->info);
          condition.values.push_back(info.updated.GetValue(colidx));
          if (added_shards.size() > 0) {
            ASSIGN_OR_RETURN(int status,
                             cascader.CascadeTo(next_table, shard_kind,
                                                added_shards, condition));
            ACCUM(status, this->status_);
          }
          break;
        }
        // Both (1) and (2) apply to VARIABLE.
        case InfoType::VARIABLE: {
          const VariableInfo &tmp = std::get<VariableInfo>(desc->info);

          // Get the corresponding FK columns in this table and dependent table.
          // If the value of the FK column was not changed by update, there
          // is nothing to cascade.
          int status = 0;
          size_t colidx = tmp.origin_column_index;
          size_t nextidx = tmp.column_index;

          // The condition is the same for both (1) and (2).
          // The only difference is the delta.
          Cascader::Condition condition;
          condition.column = nextidx;
          condition.values.push_back(info.updated.GetValue(colidx));
          if (info.old.GetValue(colidx) != info.updated.GetValue(colidx)) {
            // (2) takes priority.
            ASSIGN_OR_RETURN(status,
                             cascader.CascadeTo(next_table, shard_kind,
                                                info.new_shards, condition));
          } else if (added_shards.size() > 0) {
            // (2) does not apply, (1) applies instead.
            ASSIGN_OR_RETURN(
                status, cascader.CascadeTo(next_table, shard_kind, added_shards,
                                           condition));
          }
          ACCUM(status, this->status_);
          break;
        }
      }
    }
  }

  // Now do it to old shards.
  for (const auto &[next_table, desc] : this->table_.dependents) {
    const std::string &shard_kind = desc->shard_kind;
    for (const UpdateInfo &info : cascades) {
      // Find out if the shards of the updated records were changed.
      // This indiates whether (1) applies or not.
      std::unordered_set<util::ShardName> removed_shards;
      for (const util::ShardName &shard : info.old_shards) {
        if (shard.ShardKind() == shard_kind) {
          if (info.new_shards.count(shard) == 0) {
            removed_shards.insert(shard.Copy());
          }
        }
      }

      // Cascade according to dependency type.
      switch (desc->type) {
        // Only (1) applies to DIRECT and TRANSITIVE.
        case InfoType::DIRECT:
        case InfoType::TRANSITIVE: {
          // Get the corresponding FK columns in this table and dependent table.
          size_t colidx = desc->upcolumn_index();

          Cascader::Condition condition;
          condition.column = EXTRACT_VARIANT(column_index, desc->info);
          condition.values.push_back(info.updated.GetValue(colidx));
          if (removed_shards.size() > 0) {
            ASSIGN_OR_RETURN(int status,
                             cascader.CascadeOut(next_table, shard_kind,
                                                 removed_shards, condition));
            ACCUM(status, this->status_);
          }
          break;
        }
        // Both (1) and (2) apply to VARIABLE.
        case InfoType::VARIABLE: {
          const VariableInfo &tmp = std::get<VariableInfo>(desc->info);

          // Get the corresponding FK columns in this table and dependent table.
          // If the value of the FK column was not changed by update, there
          // is nothing to cascade.
          int status = 0;
          size_t colidx = tmp.origin_column_index;
          size_t nextidx = tmp.column_index;

          // The condition is the same for both (1) and (2).
          // The only difference is the delta.
          Cascader::Condition condition;
          condition.column = nextidx;
          condition.values.push_back(info.old.GetValue(colidx));
          if (info.old.GetValue(colidx) != info.updated.GetValue(colidx)) {
            // (2) takes priority.
            ASSIGN_OR_RETURN(status,
                             cascader.CascadeOut(next_table, shard_kind,
                                                 info.old_shards, condition));
          } else if (removed_shards.size() > 0) {
            // (2) does not apply, (1) applies instead.
            ASSIGN_OR_RETURN(status,
                             cascader.CascadeOut(next_table, shard_kind,
                                                 removed_shards, condition));
          }
          ACCUM(status, this->status_);
          break;
        }
      }
    }
  }
  return absl::OkStatus();
}

/*
 * Main entry point for update.
 */
absl::StatusOr<sql::SqlResult> UpdateContext::Exec() {
  // Make sure table exists in the schema first.
  ASSERT_RET(this->sstate_.TableExists(this->table_name_), InvalidArgument,
             "Table does not exist");

  // Begin the transaction.
  this->db_->BeginTransaction(true);
  CHECK_STATUS(this->conn_->ctx->AddCheckpoint());

  // Execute the update.
  MOVE_OR_RETURN(Result result, this->ExecWithinTransaction());
  if (result.first < 0) {
    this->db_->RollbackTransaction();
    CHECK_STATUS(this->conn_->ctx->RollbackCheckpoint());
    return sql::SqlResult(result.first);
  }

  // Commit transaction.
  this->db_->CommitTransaction();
  CHECK_STATUS(this->conn_->ctx->CommitCheckpoint());

  // Update dataflow.
  this->dstate_.ProcessRecords(this->table_name_, std::move(result.second));

  // Return number of copies inserted.
  return sql::SqlResult(result.first);
}

absl::StatusOr<UpdateContext::Result> UpdateContext::ExecWithinTransaction() {
  Result result;

  // Check if this statement affects the ownership of records in the updated
  // table.
  std::vector<UpdateInfo> cascades;
  ASSIGN_OR_RETURN(bool modifies_sharding, this->ModifiesShardingBaseTable());
  if (modifies_sharding) {
    MOVE_OR_RETURN(cascades, this->DeleteInsert());
  } else {
    cascades = this->DirectUpdate();
  }

  // Make sure we rollback on failures.
  if (this->status_ < 0) {
    result.first = this->status_;
    return result;
  }

  // DeleteInsert() handles FK checks that do not apply to DirectUpdate().
  // Below we handle the checks that apply to both.
  // We need to make sure that:
  // (1) no dependent records have a dangling OWNED BY FKs due to this update.
  // (2) the updated records do not have dangling OWNS FK to dependents.
  // Because OWNED BY FKs only point to PK, and we do not support updating PK,
  // then (1) is always maintained. We only need to check for (2).
  for (const auto &[next_table, desc] : this->table_.dependents) {
    if (desc->type == InfoType::VARIABLE) {
      const VariableInfo &info = std::get<VariableInfo>(desc->info);
      const Table &next = this->sstate_.GetTable(next_table);

      // Get the corresponding FK columns in this table and dependent table.
      const std::string &colname = info.origin_column;
      size_t colidx = info.origin_column_index;
      size_t nextidx = info.column_index;
      ASSERT_RET(nextidx == next.schema.keys().at(0), Internal,
                 "Variable OWNS FK points to nonPK");

      // Ensure Integrity for every updated row.
      for (const UpdateInfo &cascade : cascades) {
        sqlast::Value value = cascade.updated.GetValue(colidx);
        if (cascade.old.GetValue(colidx) != value) {
          if (!this->db_->Exists(next_table, nextidx, value)) {
            return absl::InvalidArgumentError("Integrity error: " + colname);
          }
        }
      }
    }
  }

  // Cascade over dependents.
  bool modifies_dependents = this->ModifiesShardingDependentTables();
  if (this->table_.dependents.size() > 0) {
    if (modifies_sharding || modifies_dependents) {
      CHECK_STATUS(this->CascadeDependents(cascades));
      if (this->status_ < 0) {
        result.first = this->status_;
        return result;
      }
    }
  }

  // Process updates to dataflows.
  for (dataflow::Record &record : this->positives_) {
    this->negatives_.push_back(std::move(record));
  }

  result.first = this->status_;
  result.second = std::move(this->negatives_);
  return result;
}

absl::StatusOr<UpdateContext::Result> UpdateContext::UpdateAnonymize() {
  this->DirectUpdate();

  Result result;
  result.first = this->status_;
  if (this->status_ >= 0) {
    for (dataflow::Record &record : this->positives_) {
      this->negatives_.push_back(std::move(record));
    }
    result.second = std::move(this->negatives_);
  }
  return result;
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
