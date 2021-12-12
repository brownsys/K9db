// Delete statements sharding and rewriting.

#include "pelton/shards/sqlengine/delete.h"

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace delete_ {

absl::StatusOr<sql::SqlResult> Shard(const sqlast::Delete &stmt,
                                     Connection *connection, bool synchronize,
                                     bool update_flows) {
  perf::Start("Delete");
  const std::string &table_name = stmt.table_name();

  // We only read from state.
  shards::SharderState *state = connection->state->sharder_state();
  dataflow::DataFlowState *dataflow_state = connection->state->dataflow_state();

  SharedLock lock;
  if (synchronize) {
    lock = state->ReaderLock();
  }

  // If no flows read from this table, we do not need to do anything
  // to update them.
  if (!dataflow_state->HasFlowsFor(table_name)) {
    update_flows = false;
  }

  // Get the rows that are going to be deleted prior to deletion to use them
  // to update the dataflows.
  dataflow::SchemaRef schema = dataflow_state->GetTableSchema(table_name);

  // Must transform the delete statement into one that is compatible with
  // the sharded schema.
  sql::SqlResult result(static_cast<int>(0));
  if (update_flows) {
    result = sql::SqlResult(schema);
  }

  // Sharding scenarios.
  auto &exec = connection->executor;
  bool is_sharded = state->IsSharded(table_name);
  if (is_sharded) {  // is_shared == true
    // Case 3: Table is sharded!
    // The table might be sharded according to different column/owners.
    // We must delete from all these different duplicates.
    for (const auto &info : state->GetShardingInformation(table_name)) {
      // Rename the table to match the sharded name.
      sqlast::Delete cloned = update_flows ? stmt.MakeReturning() : stmt;
      cloned.table_name() = info.sharded_table_name;

      // Figure out if we need to augment the (returning) result set with the
      // shard_by column.
      const std::string &shard_kind = info.shard_kind;
      int aug_index = -1;
      if (!info.IsTransitive()) {
        aug_index = info.shard_by_index;
      }

      // Find the value assigned to shard_by column in the where clause, and
      // remove it from the where clause.
      sqlast::ValueFinder value_finder(info.shard_by);
      auto [found, user_id] = cloned.Visit(&value_finder);
      if (found) {
        if (info.IsTransitive()) {
          // Transitive sharding: look up via index.
          ASSIGN_OR_RETURN(auto &lookup,
                           index::LookupIndex(info.next_index_name, user_id,
                                              dataflow_state));
          if (lookup.size() == 1) {
            user_id = std::move(*lookup.cbegin());
            // Execute statement directly against shard.
            result.Append(
                exec.Shard(&cloned, shard_kind, user_id, schema, aug_index),
                true);
          }
        } else if (state->ShardExists(info.shard_kind, user_id)) {
          // Remove where condition on the shard by column, since it does not
          // exist in the sharded table.
          sqlast::ExpressionRemover expression_remover(info.shard_by);
          cloned.Visit(&expression_remover);
          // Execute statement directly against shard.
          result.Append(
              exec.Shard(&cloned, shard_kind, user_id, schema, aug_index),
              true);
        }

      } else {
        // The delete statement by itself does not obviously constraint a
        // shard. Try finding the shard(s) via secondary indices.
        ASSIGN_OR_RETURN(
            const auto &pair,
            index::LookupIndex(table_name, info.shard_by, stmt.GetWhereClause(),
                               state, dataflow_state));
        if (pair.first) {
          // Secondary index available for some constrainted column in stmt.
          result.Append(
              exec.Shards(&cloned, shard_kind, pair.second, schema, aug_index),
              true);
        } else {
          // Secondary index unhelpful.
          // Execute statement against all shards of this kind.
          const auto &user_ids = state->UsersOfShard(shard_kind);
          result.Append(
              exec.Shards(&cloned, shard_kind, user_ids, schema, aug_index),
              true);
        }
      }
    }
  } else {
    // Case 2: Table is not sharded.
    if (update_flows) {
      sqlast::Delete cloned = stmt.MakeReturning();
      result = exec.Default(&cloned, schema);
    } else {
      result = exec.Default(&stmt);
    }
  }

  // Delete was successful, time to update dataflows.
  if (update_flows) {
    std::vector<dataflow::Record> records = result.ResultSets().at(0).Vec();
    result = sql::SqlResult(records.size());
    dataflow_state->ProcessRecords(table_name, std::move(records));
  }

  perf::End("Delete");
  return result;
}

}  // namespace delete_
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
