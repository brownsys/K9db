// Defines the API for our SQLite-adapter.
#ifndef K9DB_PLANNER_PLANNER_H_
#define K9DB_PLANNER_PLANNER_H_

#include <memory>
#include <string>

#include "k9db/dataflow/graph.h"
#include "k9db/dataflow/state.h"

namespace k9db {
namespace planner {

// Given a query, use calcite to plan its execution, and generate
// a DataFlowGraphPartition with all the operators from that plan.
std::unique_ptr<dataflow::DataFlowGraphPartition> PlanGraph(
    dataflow::DataFlowState *state, const std::string &query);

void ShutdownPlanner();
void ShutdownPlanner(bool shutdown_jvm);

}  // namespace planner
}  // namespace k9db

#endif  // K9DB_PLANNER_PLANNER_H_
