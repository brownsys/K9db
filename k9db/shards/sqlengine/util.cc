#include "k9db/shards/sqlengine/util.h"

#include <memory>
#include <utility>

#include "k9db/util/status.h"

namespace k9db {
namespace shards {
namespace sqlengine {

// Insert the records matched by condition in that given table to the given
// shards and recurse on their dependent tables.
// Affected records that were stored in the default shard are moved and not
// copied.
absl::StatusOr<int> Cascader::CascadeTo(const std::string &table_name,
                                        const std::string &shard_kind,
                                        const ShardsSet &shards,
                                        const Condition &condition) {
  // Assign the matching records to this shard.
  // AssignToShards() locates the records, does not copy any records that
  // are already in the target shard, and cleans up the default shard from
  // records that were in it and got moved out.
  sql::ResultSetAndStatus pair = this->db_->AssignToShards(
      table_name, condition.column, condition.values, shards);
  int status = pair.second;
  if (status < 0) {
    return status;
  }
  const std::vector<dataflow::Record> &records = pair.first.rows();
  if (records.size() == 0) {
    return status;
  }

  // Now, we need to cascade.
  const Table &table = this->sstate_.GetTable(table_name);
  for (const auto &[next_table, desc] : table.dependents) {
    if (desc->shard_kind != shard_kind) {
      continue;
    }

    // Unreachable: we are already cascading on some table's dependent
    // so we must now be at least transitive.
    ASSERT_RET(desc->type != InfoType::DIRECT, Internal, "Cascade unreachable");

    // Find the FK key columns.
    size_t column = desc->upcolumn_index();
    size_t next_column = EXTRACT_VARIANT(column_index, desc->info);

    // Update condition.
    Condition cascade_condition;
    cascade_condition.column = next_column;
    for (const dataflow::Record &record : records) {
      cascade_condition.values.push_back(record.GetValue(column));
    }

    // Perform cascade.
    ASSIGN_OR_RETURN(int count, this->CascadeTo(next_table, shard_kind, shards,
                                                cascade_condition));
    status += count;
    if (count < 0) {
      return count;
    }
  }

  return status;
}

// Remove the records matched by condition in that given table to the given
// shards and recurse on their dependent tables.
// Records that would not be stored in any shard after removal are moved to
// the default shard.
absl::StatusOr<int> Cascader::CascadeOut(const std::string &table_name,
                                         const util::ShardName &shard,
                                         const Condition &condition) {
  const Table &table = this->sstate_.GetTable(table_name);

  // Find the records matching the given condition and are currently in the
  // shard we want to remove from.
  // Matching records outside of this shard are irrelevant.
  std::vector<sql::KeyPair> pairs;
  std::unordered_set<std::string> duplicates;
  for (const sqlast::Value &fkval : condition.values) {
    if (duplicates.insert(fkval.AsUnquotedString()).second) {
      pairs.emplace_back(shard.Copy(), fkval);
    }
  }

  // Get the records.
  std::vector<dataflow::Record> next_records =
      this->db_->GetDirect(table_name, condition.column, pairs);

  // We should not hastily remove the records from the shard, since they may
  // legit by owned by that shard in other ways independent of the operation
  // we are cascading.
  // Thus, we must first check their legit owners, and then remove if the shard
  // is not one of the owners.
  std::string shard_kind(shard.ShardKind());
  MOVE_OR_RETURN(std::vector<ShardsSet> legitimate_shards,
                 this->LocateShards(table_name, next_records));

  // Filter out records that do not need removal because they are legitimate.
  std::vector<dataflow::Record> remove;
  std::vector<bool> unowned;
  std::unordered_set<sqlast::Value> orphans;
  for (size_t i = 0; i < next_records.size(); i++) {
    if (legitimate_shards.at(i).count(shard) == 0) {
      remove.push_back(std::move(next_records.at(i)));
      unowned.push_back(false);
      if (legitimate_shards.at(i).size() == 0) {
        sqlast::Value PK = remove.back().GetValue(table.schema.keys().at(0));
        std::string str = PK.AsUnquotedString();
        std::unordered_set<std::string> &moved = this->moved_[table_name];
        if (moved.insert(std::move(str)).second) {
          unowned.back() = true;
          orphans.insert(PK);
        }
      }
    }
  }

  // Remove records from shard, moving the ones with no owners to default shard.
  int status = this->db_->DeleteFromShard(table_name, shard, remove, unowned);
  if (status < 0) {
    return status;
  }

  // Track orphans.
  CHECK_STATUS(this->conn_->ctx->AddOrphans(table_name, orphans));

  // Now, we need to cascade onto remove dependents.
  for (const auto &[next_table, desc] : table.dependents) {
    if (desc->shard_kind != shard_kind) {
      continue;
    }

    // Unreachable: we are already cascading on some table's dependent
    // so we must now be at least transitive.
    ASSERT_RET(desc->type != InfoType::DIRECT, Internal, "Cascade unreachable");

    // Find the FK key columns.
    size_t column = desc->upcolumn_index();
    size_t next_column = EXTRACT_VARIANT(column_index, desc->info);

    // Update condition.
    Condition cascade_condition;
    cascade_condition.column = next_column;
    for (const dataflow::Record &record : remove) {
      cascade_condition.values.push_back(record.GetValue(column));
    }

    // Perform cascade.
    ASSIGN_OR_RETURN(int count,
                     this->CascadeOut(next_table, shard, cascade_condition));
    status += count;
    if (count < 0) {
      return count;
    }
  }

  return status;
}

// Helper: set of shards.
absl::StatusOr<int> Cascader::CascadeOut(const std::string &table_name,
                                         const std::string &shard_kind,
                                         const ShardsSet &shards,
                                         const Condition &condition) {
  int status = 0;
  for (const util::ShardName &shard : shards) {
    ASSIGN_OR_RETURN(int r, this->CascadeOut(table_name, shard, condition));
    status += r;
    if (r < 0) {
      return r;
    }
  }
  return status;
}

// Locates the legitimate shards of the given records.
absl::StatusOr<std::vector<ShardsSet>> Cascader::LocateShards(
    const std::string &table_name,
    const std::vector<dataflow::Record> &records) const {
  std::vector<ShardsSet> shards;

  // Need to look at each way of sharding this table.
  const Table &table = this->sstate_.GetTable(table_name);
  for (const std::unique_ptr<ShardDescriptor> &desc : table.owners) {
    // Identify the ownership FK columns and parent table.
    size_t column = EXTRACT_VARIANT(column_index, desc->info);
    size_t parent_column = desc->upcolumn_index();
    const std::string &parent_table = desc->next_table();

    // Build parent condition.
    Condition condition;
    condition.column = parent_column;
    for (const dataflow::Record &record : records) {
      condition.values.push_back(record.GetValue(column));
    }

    // Look up owners of the parents.
    CHECK_STATUS(this->LocateShards(parent_table, condition, &shards));
  }

  return shards;
}

// Locates the shards that contain records that match the given condition.
absl::Status Cascader::LocateShards(const std::string &table_name,
                                    const Condition &condition,
                                    std::vector<ShardsSet> *output) const {
  // If vector is empty, fill it with empty sets for each value in condition.
  if (output->size() == 0) {
    for (size_t i = 0; i < condition.values.size(); i++) {
      output->emplace_back();
    }
  }
  ASSERT_RET(output->size() == condition.values.size(), Internal,
             "LocateShards bad vector");

  // Go one value at a time, find the shards of that value.
  for (size_t i = 0; i < condition.values.size(); i++) {
    const sqlast::Value &value = condition.values.at(i);
    ShardsSet tmp = this->db_->FindShards(table_name, condition.column, value);
    for (const util::ShardName &shard : tmp) {
      if (shard.ShardKind() != DEFAULT_SHARD) {
        output->at(i).insert(shard.Copy());
      }
    }
  }
  return absl::OkStatus();
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
