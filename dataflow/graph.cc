#include "dataflow/graph.h"

DataFlowGraph::DataFlowGraph() {}

bool DataFlowGraph::AddNode(OperatorType type, Operator* op) {
  NodeIndex idx = mint_node_index();

  auto res = nodes_.emplace(idx, op);

  return res.second;
}
