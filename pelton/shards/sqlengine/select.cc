// SELECT statements sharding and rewriting.
#include "pelton/shards/sqlengine/select.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/upgradable_lock.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace select {

namespace {

bool IsIntLiteral(const std::string &str) {
  for (char c : str) {
    if (c < '0' || c > '9') {
      return false;
    }
  }
  return true;
}

bool IsStringLiteral(const std::string &str) {
  size_t e = str.size() - 1;
  return (str[0] == '"' || str[0] == '\'') && (str[e] == '"' || str[e] == '\'');
}

dataflow::SchemaRef ResultSchema(const sqlast::Select &stmt,
                                 dataflow::SchemaRef table_schema) {
  const std::vector<std::string> &column_names = stmt.GetColumns();
  if (column_names.at(0) == "*") {
    return table_schema;
  }

  std::vector<sqlast::ColumnDefinition::Type> column_types;
  column_types.reserve(column_names.size());
  for (const std::string &col : column_names) {
    if (IsIntLiteral(col)) {
      column_types.push_back(sqlast::ColumnDefinition::Type::INT);
    } else if (IsStringLiteral(col)) {
      column_types.push_back(sqlast::ColumnDefinition::Type::TEXT);
    } else {
      column_types.push_back(table_schema.TypeOf(table_schema.IndexOf(col)));
    }
  }
  return dataflow::SchemaFactory::Create(column_names, column_types, {});
}

}  // namespace

absl::StatusOr<sql::SqlResult> Shard(const sqlast::Select &stmt,
                                     Connection *connection, bool synchronize) {
  perf::Start("Select");
  shards::SharderState *state = connection->state->sharder_state();
  dataflow::DataFlowState *dataflow_state = connection->state->dataflow_state();
  SharedLock lock;
  if (synchronize) {
    lock = state->ReaderLock();
  }

  // Disqualifiy LIMIT and OFFSET queries.
  if (!stmt.SupportedByShards()) {
    return absl::InvalidArgumentError("Query contains unsupported features");
  }

  // Table name to select from.
  const std::string &table_name = stmt.table_name();
  dataflow::SchemaRef schema =
      ResultSchema(stmt, dataflow_state->GetTableSchema(table_name));

  sql::SqlResult result(schema);
  auto &exec = connection->executor;
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    // Case 1: table is not in any shard.
    result = exec.Default(&stmt, schema);

  } else {  // is_sharded == true
    // Case 2: table is sharded.
    // We will query all the different ways of sharding the table (for
    // duplicates with many owners), de-duplication occurs later in the
    // pipeline.
    // When there are multiple ways of sharding the table (e.g. multiple infos),
    // that means that the table represents shared data between users.
    // E.g. direct messages between a sender and receiver.
    // Data is intentionally duplicated among these shards and must be
    // deduplicated before returning query result.
    for (const auto &info : state->GetShardingInformation(table_name)) {
      // Rename the table to match the sharded name.
      sqlast::Select cloned = stmt;
      cloned.table_name() = info.sharded_table_name;

      // Figure out if we need to augment the result set with the shard_by
      // column. This columns is not physically stored in the shards because its
      // value is trivially deduced from the containing shard. Queries may still
      // require this column to be in the result, since they are written against
      // the unsharded logical schema.
      const std::string &shard_kind = info.shard_kind;
      int aug_index = -1;
      if (!info.IsTransitive()) {
        if (cloned.GetColumns().at(0) == "*") {
          aug_index = info.shard_by_index;
        } else {
          aug_index = cloned.RemoveColumn(info.shard_by);
        }
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
        } else if (state->ShardExists(shard_kind, user_id)) {
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
        // The select statement by itself does not obviously constraint a shard.
        // Try finding the shard(s) via secondary indices.
        ASSIGN_OR_RETURN(
            const auto &pair,
            index::LookupIndex(table_name, info.shard_by, stmt.GetWhereClause(),
                               state, dataflow_state));
        if (pair.first) {
          if (stmt.table_name().find("stories") != std::string::npos ||
              cloned.table_name().find("stories") != std::string::npos) {
            if (pair.second.size() == 0) {
	      sqlast::Stringifier stringifier("");
              LOG(WARNING) << "Could not find stories in index " << stringifier.Visit(&cloned);
	    }
	  }
          // Secondary index available for some constrainted column in stmt.
          result.Append(
              exec.Shards(&cloned, shard_kind, pair.second, schema, aug_index),
              true);
        } else {
          // Secondary index unhelpful.
          // Select from all the relevant shards.
          const auto &user_ids = state->UsersOfShard(shard_kind);
          if (user_ids.size() > 0) {
            sqlast::Stringifier stringifier("");
            LOG(WARNING) << "Query over table " << stmt.table_name()
                         << " executed over all shards, this will be slow!"
                         << " the query: " << stmt.Visit(&stringifier);
          }
          result.Append(
              exec.Shards(&cloned, shard_kind, user_ids, schema, aug_index),
              true);
        }
      }
    }
  }

  perf::End("Select");
  return result;
}

}  // namespace select
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
