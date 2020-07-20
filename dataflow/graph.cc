#include "dataflow/graph.h"

#include <memory>

namespace dataflow {

DataFlowGraph::DataFlowGraph() {}

bool DataFlowGraph::AddNode(OperatorType type, std::shared_ptr<Operator> op) {
  NodeIndex idx = mint_node_index();

  auto res = nodes_.emplace(idx, op);

  return res.second;
}

}  // namespace dataflow
