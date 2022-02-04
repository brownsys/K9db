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
    const dataflow::DataFlowGraph &flow, const std::string &value) {
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
      lookup_key.AddValue(dataflow::Value::Dequote(value));
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

std::vector<dataflow::Record> LookupIndexRecords(
    const std::string &index_name, const std::string &value,
    Connection *connection) {
  // Find flow.
  dataflow::DataFlowState *dataflow_state = connection->state->dataflow_state();
  const dataflow::DataFlowGraph &flow = dataflow_state->GetFlow(index_name);

  return LookupIndexRecords(flow, value);
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

// This is a function that is very specifically designed to be used by the
// deletion handling code for variable owners to figure out if a pointed-to
// resource needs to be deleted from the user shard and if it needs to be moved
// to a global database. While the function is designed specifically for this
// purpose it relies on a specific structure of the indices, and thus I decided
// to put it here, instead of having distant parts of the code relying on
// invariants of index layout.
//
// The function specifically answers the question whether a given resource is a)
// present in the user shard (if not returns MISSING) b) whether the copy in
// *this* users shard is the last copy globally (OWNS_LAST_COPY) c) whether it
// is the last copy *for this user* (LAST_COPY) or d) if there are other copies
// of this record in the users shard.
//
// Querying this index *should* be correct because pelton stores copies in the
// user shard of there are multiple relations leading from the user to the
// resource, and hwnce this index should be the ultimate authority on the cound
// of copies *with regard to this sharding of the resource*.
absl::StatusOr<RecordStatus> RecordStatusForUser(
    const std::string &index_name, const std::string &value,
    const std::string &user_id, Connection *connection) {
  uint64_t count = 0;
  uint64_t user_copies = 0;
  dataflow::DataFlowState *dataflow_state = connection->state->dataflow_state();
  const dataflow::DataFlowGraph &flow = dataflow_state->GetFlow(index_name);

  const dataflow::Value &user_id_v = dataflow::Value(flow.output_schema().TypeOf(1), user_id);

  for (const dataflow::Record &record: LookupIndexRecords(flow, value)) {
    uint64_t this_count = record.GetUInt(2);
    bool is_eq = record.GetValue(1) == user_id_v;
    if (is_eq) {
      CHECK_EQ(user_copies, 0);
      user_copies += this_count;
    } else {
      count += this_count;
    }
  }
  if (user_copies == 0) {
    return RecordStatus::MISSING;
  } else if (user_copies == 1) {
    if (count == 0) {
      return RecordStatus::OWNS_LAST_COPY;
    } else {
      return RecordStatus::LAST_COPY;
    }
  } else {
    return RecordStatus::MULTI_COPY;
  }
}

std::ostream &operator<<(std::ostream &os, const RecordStatus &st) {
  switch (st) {
    case RecordStatus::MISSING:
      return os << "MISSING";
    case RecordStatus::LAST_COPY:
      return os << "LAST_COPY";
    case RecordStatus::OWNS_LAST_COPY:
      return os << "OWNS_LAST_COPY";
    case RecordStatus::MULTI_COPY:
      return os << "MULTI_COPY";
    default:
      LOG(FATAL) << "Unknown record status variant";
  }
}

}  // namespace index
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
