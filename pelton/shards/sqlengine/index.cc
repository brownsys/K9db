// Creation and management of secondary indices.
#include "pelton/shards/sqlengine/index.h"

#include <string>

#include "pelton/shards/sqlengine/view.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace index {

absl::Status CreateIndex(const sqlast::CreateIndex &stmt, SharderState *state,
                         dataflow::DataFlowState *dataflow_state) {
  perf::Start("Create Index");
  const std::string &table_name = stmt.table_name();

  sqlast::Stringifier stringifier;
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    std::string create_str = stmt.Visit(&stringifier);
    state->connection_pool().ExecuteDefault(
        ConnectionPool::Operation::STATEMENT, create_str);

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
      std::string create_str = create_index.Visit(&stringifier);

      // The global index over all the shards.
      std::string query;
      if (unique) {
        query = "SELECT " + column_name + ", " + info.shard_by + " FROM " +
                table_name;
      } else {
        query = "SELECT " + column_name + ", " + info.shard_by +
                ", COUNT(*) FROM " + table_name + " GROUP BY " + column_name +
                ", " + info.shard_by;
      }
      sqlast::CreateView create_view{index_name, query};

      // Install flow.
      CHECK_STATUS(view::CreateView(create_view, state, dataflow_state));

      // Store the index metadata.
      state->CreateIndex(info.shard_kind, table_name, column_name,
                         info.shard_by, index_name, create_str);
    }
  }

  perf::End("Create Index");
  return absl::OkStatus();
}

}  // namespace index
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
