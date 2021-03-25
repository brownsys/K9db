// Creation and management of dataflows.

#include "pelton/shards/sqlengine/view.h"

#include <cstring>
#include <string>
#include <vector>

#include "pelton/planner/planner.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace view {

namespace {

void CopyColumns(const std::vector<std::string> cols, char **out) {
  for (size_t i = 0; i < cols.size(); i++) {
    const std::string &str = cols.at(i);
    out[i] = new char[str.size() + 1];
    // NOLINTNEXTLINE
    strcpy(out[i], str.c_str());
  }
}

void CopyValues(const dataflow::Record &record, char **out) {
  for (size_t i = 0; i < record.schema().size(); i++) {
    std::string str = record.GetValueString(i);
    out[i] = new char[str.size() + 1];
    // NOLINTNEXTLINE
    strcpy(out[i], str.c_str());
  }
}

void DeleteValues(size_t n, char **vs) {
  for (size_t i = 0; i < n; i++) {
    delete[] vs[i];
  }
}

}  // namespace

absl::Status CreateView(const sqlast::CreateView &stmt, SharderState *state,
                        dataflow::DataFlowState *dataflow_state) {
  // Plan the query using calcite and generate a concrete graph for it.
  dataflow::DataFlowGraph graph =
      planner::PlanGraph(dataflow_state, stmt.query());

  // Add The flow to state so that data is fed into it on INSERT/UPDATE/DELETE.
  dataflow_state->AddFlow(stmt.view_name(), graph);

  return absl::OkStatus();
}

absl::Status SelectView(const sqlast::Select &stmt, SharderState *state,
                        dataflow::DataFlowState *dataflow_state,
                        const OutputChannel &output) {
  // Get the corresponding flow.
  const std::string &view_name = stmt.table_name();
  const dataflow::DataFlowGraph &flow = dataflow_state->GetFlow(view_name);

  // Only support flow ending with a single output matview for now.
  if (flow.outputs().size() == 0) {
    return absl::InvalidArgumentError("Select from flow with no matviews");
  } else if (flow.outputs().size() > 1) {
    return absl::InvalidArgumentError("Select from flow with several matviews");
  }

  // Iterate over records and pass them to host callback.
  size_t colnum = 0;
  char **colnames = nullptr;
  char **colvals = nullptr;
  for (auto matview : flow.outputs()) {
    for (const auto &key : matview->Keys()) {
      for (const auto &record : matview->Lookup(key)) {
        if (colnames == nullptr) {
          std::vector<std::string> cols = record.schema().column_names();
          colnum = cols.size();
          colnames = new char *[colnum];
          colvals = new char *[colnum];
          CopyColumns(cols, colnames);
        }

        CopyValues(record, colvals);
        output.callback(output.context, colnum, colvals, colnames);
        DeleteValues(colnum, colvals);
      }
    }
  }

  if (colnames != nullptr) {
    DeleteValues(colnum, colnames);
    delete[] colnames;
    delete[] colvals;
  }

  return absl::OkStatus();
}

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
