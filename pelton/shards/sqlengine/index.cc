// Creation and management of secondary indices.

#include "pelton/shards/sqlengine/index.h"

#include <memory>

#include "absl/status/status.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/record.h"
#include "pelton/shards/sqlengine/view.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace index {

absl::StatusOr<sql::SqlResult> CreateIndex(const sqlast::CreateIndex &stmt,
                                           Connection *connection) {
  perf::Start("Create Index");
  shards::SharderState *state = connection->pelton_state->GetSharderState();
  const std::string &table_name = stmt.table_name();

  sql::SqlResult result = sql::SqlResult(false);
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    result = connection->executor_.ExecuteDefault(&stmt);

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
      CHECK_STATUS(view::CreateView(create_view, connection));

      // Store the index metadata.
      state->CreateIndex(info.shard_kind, table_name, column_name,
                         info.shard_by, index_name, create_index, unique);
    }

    result = sql::SqlResult(true);
  }

  perf::End("Create Index");
  return result;
}

absl::StatusOr<std::pair<bool, std::unordered_set<UserId>>> LookupIndex(
    const std::string &table_name, const std::string &shard_by,
    const sqlast::BinaryExpression *where_clause, SharderState *state,
    dataflow::DataFlowState *dataflow_state) {
  if (where_clause == nullptr) {
    return std::make_pair(false, std::unordered_set<UserId>{});
  }

  perf::Start("Lookup Index1");
  // Get all the columns that have a secondary index.
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
                   LookupIndex(index_flow, column_value, dataflow_state));

    perf::End("Lookup Index1");
    return std::make_pair(true, std::move(shards));
  }
  if (indexed_cols.size() == 0) {
    delete &indexed_cols;
  }

  perf::End("Lookup Index1");
  return std::make_pair(false, std::unordered_set<UserId>{});
}

absl::StatusOr<std::unordered_set<UserId>> LookupIndex(
    const std::string &index_name, const std::string &value,
    dataflow::DataFlowState *dataflow_state) {
  perf::Start("Lookup Index");
  // Find flow.
  const std::shared_ptr<dataflow::DataFlowGraph> &flow =
      dataflow_state->GetFlow(index_name);
  if (flow->outputs().size() == 0) {
    return absl::InvalidArgumentError("Read from index with no matviews");
  } else if (flow->outputs().size() > 1) {
    return absl::InvalidArgumentError("Read from index with several matviews");
  }

  // Materialized view that we want to look up in.
  auto matview = flow->outputs().at(0);

  // Construct look up key.
  dataflow::Key lookup_key{1};
  switch (matview->output_schema().TypeOf(0)) {
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
                 << matview->output_schema().TypeOf(0);
  }

  // Lookup.
  std::unordered_set<UserId> result;
  for (const dataflow::Record &record : matview->Lookup(lookup_key)) {
    result.insert(record.GetValueString(1));
  }

  perf::End("Lookup Index");
  return result;
}

}  // namespace index
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
