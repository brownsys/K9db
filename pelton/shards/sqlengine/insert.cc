// INSERT statements sharding and rewriting.
#include "pelton/shards/sqlengine/insert.h"

#include <memory>
#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/shards/sqlengine/index.h"
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

/*
 * Helpers for inserting statement into the database by sharding type.
 */
int InsertContext::DirectInsert(dataflow::Value &&fkval,
                                const ShardDescriptor &desc) {
  std::string shard_name = fkval.AsUnquotedString();
  int res = this->db_->ExecuteInsert(this->stmt_, desc.shard_kind, shard_name);
  this->shard_names_.push_back({std::move(shard_name)});
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
  this->shard_names_.emplace_back();
  for (dataflow::Record &r : indexed) {
    ASSERT_RET(r.GetInt(2) > 0, Internal, "Index count 0");
    ASSERT_RET(!r.IsNull(1), Internal, "T Index gives NULL owner");
    std::string shard_name = r.GetValueString(1);
    ACCUM(this->db_->ExecuteInsert(this->stmt_, desc.shard_kind, shard_name),
          res);
    this->shard_names_.back().insert(std::move(shard_name));
  }

  // Inserting this row before inserting the transitive row pointing to
  // its owner is an integrity error.
  ASSERT_RET(indexed.size() > 0, InvalidArgument, "Dangling owner FK");
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
  this->shard_names_.emplace_back();
  for (dataflow::Record &r : indexed) {
    ASSERT_RET(r.GetInt(2) > 0, Internal, "Index count 0");
    ASSERT_RET(!r.IsNull(1), Internal, "T Index gives NULL owner");
    std::string shard_name = r.GetValueString(1);
    ACCUM(this->db_->ExecuteInsert(this->stmt_, desc.shard_kind, shard_name),
          res);
    this->shard_names_.back().insert(std::move(shard_name));
  }

  // This row may be inserted before the corresponding many-to-many
  // rows are inserted into the variable ownership assocation table.
  if (indexed.size() == 0) {
    ACCUM(this->db_->ExecuteInsert(this->stmt_, desc.shard_kind, DEFAULT_SHARD),
          res);
    this->shard_names_.back().insert(DEFAULT_SHARD);
  }
  return res;
}

/*
 * Entry point for inserting into the database.
 */
absl::StatusOr<int> InsertContext::InsertIntoBaseTable() {
  this->shard_names_ = {};

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
  if (this->table_.owners.size() == 0) {
    ACCUM(this->db_->ExecuteInsert(this->stmt_, DEFAULT_SHARD, DEFAULT_SHARD),
          res);
  }

  return res;
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

  // The insert into this table may affect the sharding of data in dependent
  // tables. Figure this out.
  for (size_t i = 0; i < this->table_.dependents.size(); i++) {
    const auto &[dependent_table, desc] = this->table_.dependents.at(i);
    // std::unordered_set<std::string> &shard_names = this->shard_names_.at(i);
    // Only dependent tables for which this table acts as the variable ownership
    // association table can have their data affected by this insert.
    if (desc->type == InfoType::VARIABLE) {
      // TODO(babman): do the recursive copy/move.
      // Need to copy records in dependent_table that the inserted record points
      // to into the shard_names.
      // Select or delete the record, then insert it into shard.
    }
  }

  // Everything has been inserted; feed to dataflows.
  this->dstate_.ProcessRecords(this->table_name_, std::move(records));

  // Return number of copies inserted.
  return sql::SqlResult(result);
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
