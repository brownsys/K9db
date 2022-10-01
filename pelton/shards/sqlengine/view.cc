// Creation and management of dataflows.
#include "pelton/shards/sqlengine/view.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/state.h"
#include "pelton/planner/planner.h"
#include "pelton/sql/abstract_connection.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace view {

absl::StatusOr<sql::SqlResult> CreateView(const sqlast::CreateView &stmt,
                                          Connection *connection,
                                          util::UniqueLock *lock) {
  dataflow::DataFlowState &dataflow_state = connection->state->DataflowState();
  sql::AbstractConnection *db = connection->state->Database();

  // Plan the query using calcite and generate a concrete graph for it.
  std::unique_ptr<dataflow::DataFlowGraphPartition> graph =
      planner::PlanGraph(&dataflow_state, stmt.query());

  // Add The flow to state so that data is fed into it on INSERT/UPDATE/DELETE.
  std::string flow_name = stmt.view_name();
  dataflow_state.AddFlow(flow_name, std::move(graph));

  // Update new View with existing data in tables.
  std::vector<std::pair<std::string, std::vector<dataflow::Record>>> data;

  // Iterate through each table in the new View's flow.
  dataflow::Future future(true);
  for (const auto &table_name : dataflow_state.GetFlow(flow_name).Inputs()) {
    // Generate and execute select * query for current table
    pelton::sqlast::Select select{table_name};
    select.AddColumn("*");
    sql::SqlResultSet result = db->ExecuteSelect(select);

    // Vectorize and store records.
    std::vector<pelton::dataflow::Record> records = std::move(result.Vec());

    // Records will be negative here, need to turn them to positive records.
    for (auto &record : records) {
      record.SetPositive(true);
    }

    dataflow_state.ProcessRecordsByFlowName(
        flow_name, table_name, std::move(records), future.GetPromise());
  }
  future.Wait();

  return sql::SqlResult(true);
}

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
