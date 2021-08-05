// Defines the API for our SQLite-adapter.
#ifndef PELTON_PLANNER_PLANNER_H_
#define PELTON_PLANNER_PLANNER_H_

#include <memory>
#include <string>

#include "pelton/dataflow/engine.h"
#include "pelton/dataflow/graph.h"

namespace pelton {
namespace planner {

// Given a query, use calcite to plan its execution, and generate
// a DataFlowGraph with all the operators from that plan.
std::shared_ptr<dataflow::DataFlowGraph> PlanGraph(
    dataflow::DataFlowEngine *engine, const std::string &query);

void ShutdownPlanner();

}  // namespace planner
}  // namespace pelton

#endif  // PELTON_PLANNER_PLANNER_H_
