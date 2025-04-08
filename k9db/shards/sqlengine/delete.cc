// DELETE statements sharding and rewriting.
#include "k9db/shards/sqlengine/delete.h"

#include <memory>

#include "k9db/dataflow/record.h"
#include "k9db/policy/policy_engine.h"
#include "k9db/shards/sqlengine/util.h"
#include "k9db/util/iterator.h"
#include "k9db/util/shard_name.h"
#include "k9db/util/status.h"

namespace k9db {
namespace shards {
namespace sqlengine {

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
 * Check FK integrity.
 */
absl::Status DeleteContext::CheckFKIntegrity(const sql::SqlDeleteSet &delset) {
  for (const dataflow::Record &record : delset.Rows()) {
    // Ensure no rows still point to this record using OWNED_BY columns.
    for (const auto &[next_table, desc] : this->table_.dependents) {
      // We need to check this for DIRECT and TRANSITIVE.
      // We do not need to check this for VARIABLE (opposite direction FK).
      InfoType type = desc->type;
      if (type == InfoType::DIRECT || type == InfoType::TRANSITIVE) {
        // Get the corresponding FK columns in this table and dependent table.
        const std::string &colname = desc->upcolumn();
        size_t colidx = desc->upcolumn_index();
        size_t depcolidx = EXTRACT_VARIANT(column_index, desc->info);
        ASSERT_RET(colidx == this->schema_.keys().at(0), Internal,
                   "OWNED_BY FK points to nonPK");

        // Ensure no such records exist.
        sqlast::Value fkval = record.GetValue(colidx);
        if (this->db_->Exists(next_table, depcolidx, fkval)) {
          return absl::InvalidArgumentError("Integrity error1: " + colname);
        }
      }
    }

    // Ensure no rows still point to this record using OWNS columns.
    for (const std::unique_ptr<ShardDescriptor> &desc : this->table_.owners) {
      if (desc->type == InfoType::VARIABLE) {
        const VariableInfo &info = std::get<VariableInfo>(desc->info);

        // Get the corresponding FK columns in this table and dependent table.
        size_t colidx = info.column_index;
        size_t depcolidx = info.origin_column_index;
        ASSERT_RET(colidx == this->schema_.keys().at(0), Internal,
                   "Variable OWNS FK points to nonPK");

        // Ensure no such records exist.
        sqlast::Value fkval = record.GetValue(colidx);
        if (this->db_->Exists(info.origin_relation, depcolidx, fkval)) {
          return absl::InvalidArgumentError("Integrity error2: " + info.column);
        }
      }
    }
  }

  return absl::OkStatus();
}

/*
 * Recursively moving/copying records in dependent tables into the affected
 * shard.
 */
absl::StatusOr<int> DeleteContext::CascadeDependents(
    const sql::SqlDeleteSet &delset) {
  int result = 0;

  // Group by removed shard.
  Cascader cascader(this->conn_, this->lock_);
  for (const util::ShardName &shard : delset.IterateShards()) {
    for (const auto &[next_table, desc] : this->table_.dependents) {
      if (shard.ShardKind() != desc->shard_kind) {
        continue;
      }

      // No need to cascade over DIRECT and TRANSITIVE by FK integrity.
      if (desc->type == InfoType::VARIABLE) {
        const VariableInfo &info = std::get<VariableInfo>(desc->info);
        const Table &next = this->sstate_.GetTable(next_table);

        // Get the corresponding FK columns in this table and dependent table.
        size_t colidx = info.origin_column_index;
        size_t nextidx = info.column_index;
        ASSERT_RET(nextidx == next.schema.keys().at(0), Internal,
                   "Variable OWNS FK points to nonPK");

        // The value of the column that is a foreign key.
        Cascader::Condition cond;
        cond.column = nextidx;
        for (const dataflow::Record &record : delset.Iterate(shard)) {
          cond.values.push_back(record.GetValue(colidx));
        }

        // Ask database to remove from this shard all the records in the next
        // table whose <nextidx> column IN <vals>, provided they are not owned
        // by this user in some other way.
        std::unordered_set<util::ShardName> shards;
        shards.emplace(shard.ShardKind(), shard.UserID());
        ACCUM_STATUS(
            cascader.CascadeOut(next_table, desc->shard_kind, shards, cond),
            result);
      }
    }
  }

  return result;
}

/*
 * Main entry point for delete: Executes the statement against the shards.
 */
absl::StatusOr<sql::SqlResult> DeleteContext::Exec() {
  // Make sure table exists in the schema first.
  ASSERT_RET(this->sstate_.TableExists(this->table_name_), InvalidArgument,
             "Table does not exist");

  // Begin the transaction.
  this->db_->BeginTransaction(true);
  CHECK_STATUS(this->conn_->ctx->AddCheckpoint());

  // Execute the delete.
  MOVE_OR_RETURN(Result result, this->ExecWithinTransaction());
  if (result.first < 0) {
    this->db_->RollbackTransaction();
    CHECK_STATUS(this->conn_->ctx->RollbackCheckpoint());
    return sql::SqlResult(result.first);
  }

  // Commit transaction.
  this->db_->CommitTransaction();
  CHECK_STATUS(this->conn_->ctx->CommitCheckpoint());

  // Add policies to records.
  policy::MakePolicies(this->table_name_, this->conn_, &result.second);

  // Process updates to dataflows.
  this->dstate_.ProcessRecords(this->table_name_, std::move(result.second));

  // Return number of copies inserted.
  return sql::SqlResult(result.first);
}

absl::StatusOr<DeleteContext::Result> DeleteContext::ExecWithinTransaction() {
  Result result;

  // Execute the delete in the DB.
  sql::SqlDeleteSet resultset = this->db_->ExecuteDelete(this->stmt_);
  size_t status = resultset.Count();

  // Decode the deleted records.
  // Ensure no incoming FKs point to any of these records.
  CHECK_STATUS(this->CheckFKIntegrity(resultset));

  // Cascade over dependents tables and removes dependents rows from affected
  // shards.
  ASSIGN_OR_RETURN(size_t cascaded, this->CascadeDependents(resultset));
  if (cascaded < 0) {
    result.first = cascaded;
    return result;
  }

  result.first = status + cascaded;
  result.second = resultset.Vec();
  return result;
}
absl::StatusOr<DeleteContext::Result> DeleteContext::DeleteAnonymize() {
  // Execute the delete in the DB.
  sql::SqlDeleteSet resultset = this->db_->ExecuteDelete(this->stmt_);
  Result result;
  result.first = resultset.Count();
  result.second = resultset.Vec();
  return result;
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
