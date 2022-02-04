// Creation and management of secondary indices.

#include "pelton/shards/sqlengine/index.h"

#include <memory>

#include "absl/status/status.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/record.h"
#include "pelton/shards/sqlengine/view.h"
#include "pelton/shards/upgradable_lock.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace index {

namespace {

std::vector<dataflow::Record> LookupIndexRecords(
    const std::string &index_name, const std::string &value,
    Connection *connection) {
  // Find flow.
  dataflow::DataFlowState *dataflow_state = connection->state->dataflow_state();
  const dataflow::DataFlowGraph &flow = dataflow_state->GetFlow(index_name);

  // Construct look up key.
  dataflow::Key lookup_key{1};
  switch (flow.output_schema().TypeOf(0)) {
    case sqlast::ColumnDefinition::Type::UINT:
      lookup_key.AddValue(static_cast<uint64_t>(std::stoull(value)));
      break;
    case sqlast::ColumnDefinition::Type::INT:
      lookup_key.AddValue(static_cast<int64_t>(std::stoll(value)));
      break;
    case sqlast::ColumnDefinition::Type::TEXT: {
      lookup_key.AddValue(dataflow::Record::Dequote(value));
      break;
    }
    case sqlast::ColumnDefinition::Type::DATETIME:
      lookup_key.AddValue(value);
      break;
    default:
      LOG(FATAL) << "Unsupported data type in LookupIndex: "
                 << flow.output_schema().TypeOf(0);
  }
  return flow.Lookup(lookup_key, -1, 0);
}

}

absl::StatusOr<sql::SqlResult> CreateIndex(const sqlast::CreateIndex &stmt,
                                           Connection *connection) {
  // Need a unique lock as we are changing index metadata
  UniqueLock lock = connection->state->sharder_state()->WriterLock();
  return CreateIndexStateIsAlreadyLocked(stmt, connection, &lock);
}

absl::StatusOr<sql::SqlResult> CreateIndexStateIsAlreadyLocked(
    const sqlast::CreateIndex &stmt, Connection *connection, UniqueLock *lock) {
  const std::string &table_name = stmt.table_name();
  shards::SharderState *state = connection->state->sharder_state();

  sql::SqlResult result = sql::SqlResult(false);
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    result = connection->executor.Default(&stmt);

  } else {  // is_sharded == true
    const std::string &column_name = stmt.column_name();
    const std::string &base_index_name = stmt.index_name();

    const sqlast::CreateTable &create_stmt = state->GetSchema(table_name);
    const sqlast::ColumnDefinition column = create_stmt.GetColumn(column_name);
    bool unique =
        column.HasConstraint(sqlast::ColumnConstraint::Type::PRIMARY_KEY) ||
        column.HasConstraint(sqlast::ColumnConstraint::Type::UNIQUE);

    // Create an index for every way of sharding the table.
    for (const ShardingInformation &info :
         state->GetShardingInformation(table_name)) {
      // Inner shard index.
      std::string index_name = base_index_name + "_" + info.shard_by;
      sqlast::CreateIndex create_index{index_name, info.sharded_table_name,
                                       column_name};

      // The global index over all the shards.
      std::string query;
      if (unique) {
        query = "SELECT " + column_name + ", " + info.shard_by + " FROM " +
                table_name + " WHERE " + column_name + " = ?";
      } else {
        query = "SELECT " + column_name + ", " + info.shard_by +
                ", COUNT(*) FROM " + table_name + " GROUP BY " + column_name +
                ", " + info.shard_by + " HAVING " + column_name + " = ?";
      }
      sqlast::CreateView create_view{index_name, query};

      // Install flow.
      CHECK_STATUS(view::CreateView(create_view, connection, lock));

      // Store the index metadata.
      state->CreateIndex(info.shard_kind, table_name, column_name,
                         info.shard_by, index_name, create_index, unique);
    }

    result = sql::SqlResult(true);
  }

  return result;
}

absl::StatusOr<std::pair<bool, std::unordered_set<UserId>>> LookupIndex(
    const std::string &table_name, const std::string &shard_by,
    const sqlast::BinaryExpression *where_clause, Connection *connection) {
  SharderState *state = connection->state->sharder_state();
  if (where_clause == nullptr) {
    return std::make_pair(false, std::unordered_set<UserId>{});
  }

  // Get all the columns that have a secondary index.
  if (state->HasIndicesFor(table_name)) {
    const std::unordered_set<ColumnName> &indexed_cols =
        state->IndicesFor(table_name);

    // See if the where clause conditions on a value for one of these columns.
    for (const ColumnName &column_name : indexed_cols) {
      sqlast::ValueFinder value_finder(column_name);
      auto [found, column_value] = where_clause->Visit(&value_finder);
      if (!found) {
        continue;
      }

      // The where clause gives us a value for "column_name", we can translate
      // it to some value(s) for shard_by column by looking at the index.
      const FlowName &index_flow =
          state->IndexFlow(table_name, column_name, shard_by);
      MOVE_OR_RETURN(std::unordered_set<UserId> shards,
                     LookupIndex(index_flow, column_value, connection));

      return std::make_pair(true, std::move(shards));
    }
  }

  return std::make_pair(false, std::unordered_set<UserId>{});
}

absl::StatusOr<std::unordered_set<UserId>> LookupIndex(
    const std::string &index_name, const std::string &value,
    Connection *connection) {
  // Lookup.
  std::unordered_set<UserId> result;
  for (const dataflow::Record &record : LookupIndexRecords(index_name, value, connection)) {
    result.insert(record.GetValueString(1));
  }

  return result;
}

absl::StatusOr<uint64_t> LookupIndexEntryCount(
    const std::string &index_name, const std::string &value,
    Connection *connection) {
  uint64_t count = 0;
  for (const dataflow::Record &record: LookupIndexRecords(index_name, value, connection)) {
    count += record.GetUInt(2);
  }
  return count;
}

}  // namespace index
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
