// INSERT statements sharding and rewriting.
#include "k9db/shards/sqlengine/insert.h"

#include <memory>
#include <utility>

#include "k9db/dataflow/record.h"
#include "k9db/shards/sqlengine/index.h"
#include "k9db/shards/sqlengine/util.h"
#include "k9db/util/shard_name.h"
#include "k9db/util/status.h"

namespace k9db {
namespace shards {
namespace sqlengine {

// Helpful macro: if a given int expression is negative (i.e. an error code),
// return it. Otherwise, add it to the given accumulator.
#define __ACCUM_VAR_NAME(arg) __ACCUM_STATUS##arg
#define __ACCUM_VAL(arg) __ACCUM_VAR_NAME(arg)
#define ACCUM(expr, acc)                                       \
  auto __ACCUM_VAL(__LINE__) = expr;                           \
  if (__ACCUM_VAL(__LINE__) < 0) return __ACCUM_VAL(__LINE__); \
  acc += __ACCUM_VAL(__LINE__)
#define ACCUM_STATUS(expr, acc)                                \
  ASSIGN_OR_RETURN(auto __ACCUM_VAL(__LINE__), expr);          \
  if (__ACCUM_VAL(__LINE__) < 0) return __ACCUM_VAL(__LINE__); \
  acc += __ACCUM_VAL(__LINE__)

/*
 * Helpers for inserting statement into the database by sharding type.
 */
absl::Status InsertContext::DirectInsert(sqlast::Value &&fkval,
                                         const ShardDescriptor &desc) {
  // Need to ensure that the FK points to an existing record (and lock it).
  const DirectInfo &info = std::get<DirectInfo>(desc.info);
  if (desc.shard_kind != this->table_name_) {
    const Table &next_table = this->sstate_.GetTable(desc.shard_kind);
    ASSERT_RET(info.next_column_index == next_table.schema.keys().at(0),
               Internal, "Direct OWNED_BY FK points to nonPK");
    if (!this->db_->Exists(desc.shard_kind, fkval)) {
      return absl::InvalidArgumentError("Integrity error(1): " + info.column);
    }
  }
  this->shards_.emplace(desc.shard_kind, fkval.AsUnquotedString());
  return absl::OkStatus();
}
absl::Status InsertContext::TransitiveInsert(sqlast::Value &&fkval,
                                             const ShardDescriptor &desc) {
  const TransitiveInfo &info = std::get<TransitiveInfo>(desc.info);

  // Need to ensure that the FK points to an existing record (and lock it).
  const Table &next_table = this->sstate_.GetTable(info.next_table);
  ASSERT_RET(info.next_column_index == next_table.schema.keys().at(0), Internal,
             "Transitive OWNED_BY FK points to nonPK");
  if (!this->db_->Exists(info.next_table, fkval)) {
    return absl::InvalidArgumentError("Integrity error(2): " + info.column);
  }

  // Insert into shards of users as determined via transitive index.
  const IndexDescriptor &index = *info.index;
  std::vector<dataflow::Record> indexed =
      index::LookupIndex(index, std::move(fkval), this->conn_, this->lock_);

  // We know we dont have duplicates because index does group by.
  for (dataflow::Record &r : indexed) {
    ASSERT_RET(r.GetInt(2) > 0, Internal, "Index count 0");
    ASSERT_RET(!r.IsNull(1), Internal, "T Index gives NULL owner");
    std::string user_id = r.GetValue(1).AsUnquotedString();
    this->shards_.emplace(desc.shard_kind, r.GetValue(1).AsUnquotedString());
  }
  return absl::OkStatus();
}
absl::Status InsertContext::VariableInsert(sqlast::Value &&fkval,
                                           const ShardDescriptor &desc) {
  // Because of foreign key integrity, we must be inserting this row before
  // the row in the parent table that points to this row with an OWNS FK.
  // Thus, we do not need to check if this ownership path leads to any users,
  // since it is guaranteed to be empty.
  return absl::OkStatus();
}

/*
 * Inserts the current statement to its table in the correct shards, while
 * ensuring all integrity invariants are true.
 * Does not commit, update the datalfow, or perform any cascading.
 */
absl::StatusOr<int> InsertContext::InsertIntoBaseTable() {
  // First, make sure PK does not exist! (this also locks).
  size_t pk = this->schema_.keys().front();
  const std::string &pkcol = this->schema_.NameOf(pk);

  sqlast::Value pkval = this->stmt_->GetValue(pkcol, pk);
  if (this->db_->Exists(this->table_name_, pkval)) {
    return absl::InvalidArgumentError("PK exists!");
  }

  // Then, we need to make sure that outgoing OWNS columns point to existing
  // records (and lock them)!
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

      // Ensure record exists and lock it.
      sqlast::Value fkval = this->stmt_->GetValue(colname, colidx);
      if (!this->db_->Exists(next_table, fkval)) {
        return absl::InvalidArgumentError("Integrity error(3): " + colname);
      }
    }
  }

  // Need to insert a copy for each way of sharding the table.
  for (const std::unique_ptr<ShardDescriptor> &desc : this->table_.owners) {
    // Identify value of the column along which we are sharding.
    size_t colidx = EXTRACT_VARIANT(column_index, desc->info);
    const std::string &colname = EXTRACT_VARIANT(column, desc->info);
    sqlast::Value val = this->stmt_->GetValue(colname, colidx);
    if (val.IsNull()) {
      continue;
    }

    // Handle according to sharding type.
    switch (desc->type) {
      case InfoType::DIRECT: {
        CHECK_STATUS(this->DirectInsert(std::move(val), *desc));
        break;
      }
      case InfoType::TRANSITIVE: {
        CHECK_STATUS(this->TransitiveInsert(std::move(val), *desc));
        break;
      }
      case InfoType::VARIABLE: {
        CHECK_STATUS(this->VariableInsert(std::move(val), *desc));
        break;
      }
      default:
        return absl::InternalError("Unreachable sharding case");
    }

    // We just inserted a new user!
    if (desc->shard_kind == this->table_name_) {
      this->new_users_++;
    }
  }

  // Insert into the identified shards.
  if (this->shards_.size() > 0) {
    int result = 0;
    for (const util::ShardName &shard_name : this->shards_) {
      ACCUM(this->db_->ExecuteInsert(*this->stmt_, shard_name), result);
    }
    return result;
  } else {
    // If no OWNERs detected, we insert into global/default shard.
    util::ShardName shard_name(DEFAULT_SHARD, DEFAULT_SHARD);
    int status = this->db_->ExecuteInsert(*this->stmt_, shard_name);
    // If table is owned by someone but we inserted into default shard,
    // track orphaned data in CTX.
    if (status >= 0 && this->table_.owners.size() > 0) {
      CHECK_STATUS(this->conn_->ctx->AddOrphan(this->table_name_, pkval));
    }
    return status;
  }
}

/*
 * Recursively moving/copying records in dependent tables into the affected
 * shard.
 */
absl::StatusOr<int> InsertContext::CascadeDependents() {
  int result = 0;

  // The insert into this table may affect the sharding of data in dependent
  // tables. Figure this out.
  Cascader cascader(this->conn_, this->lock_);
  for (const auto &[next_table, desc] : this->table_.dependents) {
    // Dependent tables for which this table acts as the variable ownership
    // association table can have their data affected by this insert.
    // However, other types of sharding will not (by FK integrity).
    if (desc->type == InfoType::VARIABLE) {
      const VariableInfo &info = std::get<VariableInfo>(desc->info);
      const Table &next = this->sstate_.GetTable(next_table);

      // Get the corresponding FK columns in this table and dependent table.
      size_t colidx = info.origin_column_index;
      size_t nextidx = info.column_index;
      ASSERT_RET(nextidx == next.schema.keys().at(0), Internal,
                 "Variable OWNS FK points to nonPK");

      // Find the shards to cascade records in the dependent table to.
      std::unordered_set<util::ShardName> shards;
      for (const util::ShardName &shard : this->shards_) {
        if (shard.ShardKind() == desc->shard_kind) {
          shards.emplace(shard.ShardKind(), shard.UserID());
        }
      }

      // If no shards then there is nothing to cascade.
      if (shards.size() == 0) {
        continue;
      }

      // The value of the column that is a foreign key.
      Cascader::Condition condition;
      condition.column = nextidx;
      for (const dataflow::Record &record : this->records_) {
        condition.values.push_back(record.GetValue(colidx));
      }

      // Ask database to assign all the records in the next table
      // whose <nextidx> column IN <vals>.
      ACCUM_STATUS(
          cascader.CascadeTo(next_table, desc->shard_kind, shards, condition),
          result);
    }
  }

  return result;
}

/*
 * Add auto increment and default values.
 */
absl::Status InsertContext::AutoIncrementAndDefault() {
  // Statement does not specify columns: must insert to all columns, cannot
  // have auto increments.
  if (!this->stmt_->HasColumns()) {
    ASSERT_RET(this->stmt_->GetValues().size() == this->schema_.size(),
               InvalidArgument,
               "Column-less insert statement must insert to all columns");
    ASSERT_RET(this->table_.auto_increments.size() == 0, InvalidArgument,
               "Column-less insert statement against AUTO_INCREMENT table");
    return absl::OkStatus();
  }

  // No AUTO_INCREMENT: can early return if statement inserts to all columns.
  if (this->table_.auto_increments.size() == 0) {
    if (this->stmt_->GetValues().size() == this->schema_.size()) {
      return absl::OkStatus();
    }
  }

  // Find missing columns, they must all be AUTO_INCREMENT or have a default.
  size_t found = 0;
  std::vector<sqlast::Value> values;
  for (size_t i = 0; i < this->schema_.size(); i++) {
    const std::string &column = this->schema_.NameOf(i);
    int idx = this->stmt_->HasValue(column);
    if (idx > -1) {
      found++;
      values.push_back(this->stmt_->GetValues().at(idx));
    } else {
      // column is missing, maybe it has a default or auto_increment.
      if (this->table_.auto_increments.count(column) > 0) {
        int64_t v = this->table_.auto_increments.at(column)++;
        values.emplace_back(v);
      } else if (this->table_.defaults.count(column) > 0) {
        values.push_back(this->table_.defaults.at(column));
      } else {
        return absl::InvalidArgumentError("Insert leaves a column valueless");
      }
    }
  }
  ASSERT_RET(found == this->stmt_->GetValues().size(), InvalidArgument,
             "Insert statement targets non-existing column");

  // Store new statement.
  sqlast::Insert stmt{this->table_name_};
  stmt.SetValues(std::move(values));
  this->stmt_ = RefOrOwned<sqlast::Insert>::FromOwned(std::move(stmt));
  return absl::OkStatus();
}

/*
 * Main entry point for insert: Executes the statement against the shards.
 */
absl::StatusOr<sql::SqlResult> InsertContext::Exec() {
  // Make sure table exists in the schema first.
  ASSERT_RET(this->sstate_.TableExists(this->table_name_), InvalidArgument,
             "Table does not exist");

  // Apply any auto_increment and default values.
  CHECK_STATUS(this->AutoIncrementAndDefault());

  // Begin the transaction.
  this->db_->BeginTransaction(true);
  CHECK_STATUS(this->conn_->ctx->AddCheckpoint());

  // The inserted record to feed to dataflow engine.
  this->records_.push_back(this->dstate_.CreateRecord(*this->stmt_));

  // Insert this statement into the physical shards.
  ASSIGN_OR_RETURN(int status, this->InsertIntoBaseTable());
  if (status < 0) {
    this->db_->RollbackTransaction();
    CHECK_STATUS(this->conn_->ctx->RollbackCheckpoint());
    return sql::SqlResult(status);
  }

  // Cascade any dependent records in dependent tables into the shards of
  // the inserted statement.
  int cascaded = 0;
  if (this->shards_.size() > 0) {
    ASSIGN_OR_RETURN(cascaded, this->CascadeDependents());
    if (cascaded < 0) {
      this->db_->RollbackTransaction();
      CHECK_STATUS(this->conn_->ctx->RollbackCheckpoint());
      return sql::SqlResult(cascaded);
    }
  }

  // Commit transaction.
  this->db_->CommitTransaction();
  CHECK_STATUS(this->conn_->ctx->CommitCheckpoint());

  // Process updates to dataflows.
  this->dstate_.ProcessRecords(this->table_name_, std::move(this->records_));

  // Increment number of users in the system if we inserted a new user!
  if (this->new_users_ > 0) {
    this->sstate_.IncrementUsers(this->table_name_, this->new_users_);
  }

  // Return number of copies inserted.
  return sql::SqlResult(status + cascaded);
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
