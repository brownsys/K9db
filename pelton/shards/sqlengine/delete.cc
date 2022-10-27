// DELETE statements sharding and rewriting.
#include "pelton/shards/sqlengine/delete.h"

#include <memory>

#include "pelton/dataflow/record.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/iterator.h"
#include "pelton/util/shard_name.h"
#include "pelton/util/status.h"

namespace pelton {
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
 * Delete dependent records from the shard.
 */
template <typename Iterable>
absl::StatusOr<int> DeleteContext::DeleteDependents(
    const Table &table, const util::ShardName &shard_name, Iterable &&records) {
  int result = 0;
  // The insert into this table may affect the sharding of data in dependent
  // tables. Figure this out.
  for (const auto &[next_table, desc] : table.dependents) {
    bool direct = false;
    const Table &next = this->sstate_.GetTable(next_table);
    if (desc->shard_kind != shard_name.ShardKind()) {
      continue;
    }

    size_t colidx;
    size_t nextidx;
    switch (desc->type) {
      case InfoType::VARIABLE: {
        // Dependent tables for which this table acts as the variable ownership
        // association table can have their data affected by this insert.
        const VariableInfo &info = std::get<VariableInfo>(desc->info);

        // Get the corresponding FK columns in this table and dependent table.
        colidx = info.origin_column_index;
        nextidx = info.column_index;
        ASSERT_RET(nextidx == next.schema.keys().at(0), Internal,
                   "Variable OWNS FK points to nonPK");
        break;
      }
      case InfoType::TRANSITIVE: {
        // Similar but for transitive dependencies.
        const TransitiveInfo &info = std::get<TransitiveInfo>(desc->info);

        // Get the corresponding FK columns in this table and dependent table.
        colidx = info.next_column_index;
        nextidx = info.column_index;
        ASSERT_RET(colidx == table.schema.keys().at(0), Internal,
                   "Transitive OWNER points to nonPK");
        break;
      }
      case InfoType::DIRECT: {
        const DirectInfo &info = std::get<DirectInfo>(desc->info);
        colidx = info.next_column_index;
        nextidx = info.column_index;
        ASSERT_RET(desc->shard_kind == table.table_name, Internal,
                   "Direct but to a different shard kind");
        direct = true;
        break;
      }
    }

    // The value of the column that is a foreign key.
    std::vector<sql::KeyPair> vals;
    std::unordered_set<std::string> duplicates;
    for (const dataflow::Record &record : records) {
      if (duplicates.insert(record.GetValueString(colidx)).second) {
        vals.emplace_back(shard_name.Copy(), record.GetValue(colidx));
      }
    }

    // We want to delete next[nextidx] if they are no longer owned by this
    // user due to deletion.
    std::vector<dataflow::Record> rm;
    std::vector<bool> def;

    // Get records.
    std::vector<dataflow::Record> next_records =
        this->db_->GetDirect(next_table, nextidx, vals);
    ASSERT_RET(!direct || next_records.size() == 0, Internal,
               "Cannot delete data subject with data. use GDPR FORGET");

    // Find whether or not they are owned by this user.
    std::vector<ShardLocation> locations =
        Locate(next_table, shard_name, next_records, this->conn_, this->lock_);
    size_t pk = next.schema.keys().front();
    std::unordered_set<std::string> &moved_set = this->moved_[next_table];
    for (size_t i = 0; i < next_records.size(); i++) {
      switch (locations.at(i)) {
        case ShardLocation::IN_GIVEN_SHARD:
          break;
        case ShardLocation::NOT_IN_GIVEN_SHARD: {
          rm.push_back(std::move(next_records.at(i)));
          def.push_back(false);
          break;
        }
        case ShardLocation::NO_SHARD: {
          dataflow::Record &record = next_records.at(i);
          def.push_back(moved_set.insert(record.GetValueString(pk)).second);
          rm.push_back(std::move(record));
          break;
        }
      }
    }
    if (rm.size() == 0) {
      continue;
    }

    // Delete appropriate records, and move free floating ones to default shard.
    ACCUM(this->db_->DeleteFromShard(next_table, shard_name, rm, def), result);

    // Continue recursively.
    ACCUM_STATUS(this->DeleteDependents(next, shard_name, util::Iterable(&rm)),
                 result);
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

  // Execute the delete in the DB.
  sql::SqlDeleteSet result = this->db_->ExecuteDelete(this->stmt_);

  // Update dataflow.
  std::vector<dataflow::Record> updates;
  if (this->table_.dependents.size() == 0) {
    updates = result.Vec();
  } else {
    updates.reserve(result.Rows().size());
    for (const dataflow::Record &record : result.Rows()) {
      updates.push_back(record.Copy());
    }
  }
  this->dstate_.ProcessRecords(this->table_name_, std::move(updates));

  // Recursively remove dependent records in dependent tables from the shard
  // when needed.
  int total_count = result.Count();
  for (const util::ShardName &shard : result.IterateShards()) {
    ASSIGN_OR_RETURN(int status, this->DeleteDependents(this->table_, shard,
                                                        result.Iterate(shard)));
    if (status < 0) {
      return sql::SqlResult(status);
    }
    total_count += status;
  }

  // Return number of copies inserted.
  return sql::SqlResult(total_count);
}

// Similar to exec but returns deleted records.
absl::StatusOr<std::pair<std::vector<dataflow::Record>, int>>
DeleteContext::ExecReturning() {
  // Make sure table exists in the schema first.
  ASSERT_RET(this->sstate_.TableExists(this->table_name_), InvalidArgument,
             "Table does not exist");

  // Execute the delete in the DB.
  sql::SqlDeleteSet result = this->db_->ExecuteDelete(this->stmt_);

  // Update dataflow.
  std::vector<dataflow::Record> updates;
  updates.reserve(result.Rows().size());
  for (const dataflow::Record &record : result.Rows()) {
    updates.push_back(record.Copy());
  }
  this->dstate_.ProcessRecords(this->table_name_, std::move(updates));

  // Recursively remove dependent records in dependent tables from the shard
  // when needed.
  int total_count = result.Count();
  for (const util::ShardName &shard : result.IterateShards()) {
    ASSIGN_OR_RETURN(int status, this->DeleteDependents(this->table_, shard,
                                                        result.Iterate(shard)));
    if (status < 0) {
      return std::pair(std::vector<dataflow::Record>(), status);
    }
    total_count += status;
  }

  // Return number of copies inserted.
  return std::pair(result.Vec(), total_count);
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
