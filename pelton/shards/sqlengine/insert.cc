// INSERT statements sharding and rewriting.
#include "pelton/shards/sqlengine/insert.h"

#include <memory>
#include <utility>

#include "pelton/dataflow/record.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/util/shard_name.h"
#include "pelton/util/status.h"

namespace pelton {
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

/* Have we inserted the record into this shard yet? */
bool InsertContext::InsertedInto(const std::string &shard_kind,
                                 const std::string &user_id) {
  return !this->shards_[shard_kind].emplace(shard_kind, user_id).second;
}

/*
 * Helpers for inserting statement into the database by sharding type.
 */
int InsertContext::DirectInsert(dataflow::Value &&fkval,
                                const ShardDescriptor &desc) {
  int res = 0;
  std::string user_id = fkval.AsUnquotedString();
  if (!this->InsertedInto(desc.shard_kind, user_id)) {
    util::ShardName shard_name(desc.shard_kind, user_id);
    res = this->db_->ExecuteInsert(this->stmt_, shard_name);
  }
  return res;
}
absl::StatusOr<int> InsertContext::TransitiveInsert(
    dataflow::Value &&fkval, const ShardDescriptor &desc) {
  // Insert into shards of users as determined via transitive index.
  const TransitiveInfo &info = std::get<TransitiveInfo>(desc.info);
  const IndexDescriptor &index = *info.index;
  std::vector<dataflow::Record> indexed =
      index::LookupIndex(index, std::move(fkval), this->conn_, this->lock_);

  // We know we dont have duplicates because index does group by.
  int res = 0;
  for (dataflow::Record &r : indexed) {
    ASSERT_RET(r.GetInt(2) > 0, Internal, "Index count 0");
    ASSERT_RET(!r.IsNull(1), Internal, "T Index gives NULL owner");
    std::string user_id = r.GetValueString(1);
    if (!this->InsertedInto(desc.shard_kind, user_id)) {
      util::ShardName shard_name(desc.shard_kind, user_id);
      ACCUM(this->db_->ExecuteInsert(this->stmt_, shard_name), res);
    }
  }

  // While this foreign key should point to an existing record, that record
  // may not have an owner (e.g. no variable association records for it).
  // In that acase, we insert this row to the default shard (unless other
  // ownership paths reveal an existing owner).
  return res;
}
absl::StatusOr<int> InsertContext::VariableInsert(dataflow::Value &&fkval,
                                                  const ShardDescriptor &desc) {
  const VariableInfo &info = std::get<VariableInfo>(desc.info);
  const IndexDescriptor &index = *info.index;
  std::vector<dataflow::Record> indexed =
      index::LookupIndex(index, std::move(fkval), this->conn_, this->lock_);

  // We know we dont have duplicates because index does group by.
  int res = 0;
  for (dataflow::Record &r : indexed) {
    ASSERT_RET(r.GetInt(2) > 0, Internal, "Index count 0");
    ASSERT_RET(!r.IsNull(1), Internal, "T Index gives NULL owner");
    std::string user_id = r.GetValueString(1);
    if (!this->InsertedInto(desc.shard_kind, user_id)) {
      util::ShardName shard_name(desc.shard_kind, user_id);
      ACCUM(this->db_->ExecuteInsert(this->stmt_, shard_name), res);
    }
  }

  // This row may be inserted before the corresponding many-to-many
  // rows are inserted into the variable ownership assocation table.
  // In that case, we insert this row according to any other defined ownership
  // paths, or into the default shard.
  return res;
}

/*
 * Entry point for inserting into the database.
 */
absl::StatusOr<int> InsertContext::InsertIntoBaseTable() {
  // Need to insert a copy for each way of sharding the table.
  int res = 0;
  for (const std::unique_ptr<ShardDescriptor> &desc : this->table_.owners) {
    // Identify value of the column along which we are sharding.
    size_t colidx = EXTRACT_VARIANT(column_index, desc->info);
    const std::string &colname = EXTRACT_VARIANT(column, desc->info);
    auto coltype = this->schema_.TypeOf(colidx);
    dataflow::Value val(coltype, this->stmt_.GetValue(colname, colidx));
    ASSERT_RET(!val.IsNull(), Internal, "OWNER cannot be NULL");

    // Handle according to sharding type.
    switch (desc->type) {
      case InfoType::DIRECT: {
        ACCUM(this->DirectInsert(std::move(val), *desc), res);
        break;
      }
      case InfoType::TRANSITIVE: {
        ACCUM_STATUS(this->TransitiveInsert(std::move(val), *desc), res);
        break;
      }
      case InfoType::VARIABLE: {
        ACCUM_STATUS(this->VariableInsert(std::move(val), *desc), res);
        break;
      }
      default:
        return absl::InternalError("Unreachable sharding case");
    }

    // We just inserted a new user!
    if (desc->shard_kind == this->table_name_) {
      this->sstate_.IncrementUsers(desc->shard_kind);
    }
  }

  // If no OWNERs detected, we insert into global/default shard.
  if (res == 0) {
    util::ShardName shard_name(DEFAULT_SHARD, DEFAULT_SHARD);
    ACCUM(this->db_->ExecuteInsert(this->stmt_, shard_name), res);
  }

  return res;
}

absl::StatusOr<int> InsertContext::AssignDependentsToShard(
    const Table &table, const std::vector<dataflow::Record> &records) {
  int result = 0;
  // The insert into this table may affect the sharding of data in dependent
  // tables. Figure this out.
  for (const auto &[next_table, desc] : table.dependents) {
    // Get the user ids that we need to copy records in the dependent table to.
    const auto &targets = this->shards_[desc->shard_kind];
    if (targets.size() == 0) {
      continue;
    }

    // Dependent tables for which this table acts as the variable ownership
    // association table can have their data affected by this insert.
    if (desc->type == InfoType::VARIABLE) {
      const VariableInfo &info = std::get<VariableInfo>(desc->info);
      const Table &next = this->sstate_.GetTable(next_table);

      // Get the corresponding FK columns in this table and dependent table.
      size_t colidx = info.origin_column_index;
      size_t nextidx = info.column_index;
      ASSERT_RET(nextidx == next.schema.keys().at(0), Internal,
                 "Variable OWNS FK points to nonPK");

      // The value of the column that is a foreign key.
      std::vector<dataflow::Value> vals;
      for (const dataflow::Record &record : records) {
        vals.emplace_back(record.GetValue(colidx));
      }

      // Ask database to assign all the records in the next table, whose id =
      // the FK value to the owners of the record that was just inserted.
      // This also removes any copies of the data in the default shard.
      auto &&[records, status] =
          this->db_->AssignToShards(next_table, nextidx, vals, targets);
      ACCUM(status, result);

      // Do it recursively for dependent of copied records.
      ACCUM_STATUS(this->AssignDependentsToShard(next, records.rows()), result);
    }

    // Similar but for transitive dependencies.
    if (desc->type == InfoType::TRANSITIVE) {
      const TransitiveInfo &info = std::get<TransitiveInfo>(desc->info);
      const Table &next = this->sstate_.GetTable(next_table);

      // Get the corresponding FK columns in this table and dependent table.
      size_t colidx = info.next_column_index;
      size_t nextidx = info.column_index;

      // The value of the column that is a foreign key.
      std::vector<dataflow::Value> vals;
      for (const dataflow::Record &record : records) {
        vals.emplace_back(record.GetValue(colidx));
      }

      // Ask database to assign all the records in the next table, whose id =
      // the FK value to the owners of the record that was just inserted.
      // This also removes any copies of the data in the default shard.
      auto &&[records, status] =
          this->db_->AssignToShards(next_table, nextidx, vals, targets);
      ACCUM(status, result);

      // Do it recursively for dependent of copied records.
      ACCUM_STATUS(this->AssignDependentsToShard(next, records.rows()), result);
    }
  }
  return result;
}

/*
 * Main entry point for insert: Executes the statement against the shards.
 */
absl::StatusOr<sql::SqlResult> InsertContext::Exec() {
  // Make sure table exists in the schema first.
  ASSERT_RET(this->sstate_.TableExists(this->table_name_), InvalidArgument,
             "Table does not exist");

  // The inserted / replaced records to feed to dataflow engine.
  std::vector<dataflow::Record> records;
  records.push_back(this->dstate_.CreateRecord(this->stmt_));

  // If replace, we need to delete the old record first (if exists), then
  // insert the new one.
  if (this->stmt_.replace()) {
    return absl::InvalidArgumentError("REPLACE unsupported");
  }

  // Insert the data into the physical shards.
  ASSIGN_OR_RETURN(int result, this->InsertIntoBaseTable());
  if (result < 0) {
    return sql::SqlResult(result);
  }

  // Copy/move any dependent records in dependent table to the shards of
  // users that own the just inserted record.
  ASSIGN_OR_RETURN(int status,
                   this->AssignDependentsToShard(this->table_, records));
  if (status < 0) {
    return sql::SqlResult(status);
  }

  // Everything has been inserted; feed to dataflows.
  this->dstate_.ProcessRecords(this->table_name_, std::move(records));

  // Return number of copies inserted.
  return sql::SqlResult(result + status);
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
