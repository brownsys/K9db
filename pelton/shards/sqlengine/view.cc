// Creation and management of dataflows.

#include "pelton/shards/sqlengine/view.h"

#include "pelton/planner/planner.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace view {

absl::Status CreateView(const sqlast::CreateView &stmt, SharderState *state,
                        dataflow::DataFlowState *dataflow_state) {
  // Plan the query using calcite and generate a concrete graph for it.
  dataflow::DataFlowGraph graph =
      planner::PlanGraph(dataflow_state, stmt.query());

  // Add The flow to state so that data is fed into it on INSERT/UPDATE/DELETE.
  dataflow_state->AddFlow(stmt.view_name(), graph);

  return absl::OkStatus();
}

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
