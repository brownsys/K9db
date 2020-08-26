#include "dataflow/graph.h"
#include "dataflow/edge.h"

#include <memory>

namespace dataflow {

DataFlowGraph::DataFlowGraph() {}

bool DataFlowGraph::AddNode(OperatorType type, std::shared_ptr<Operator> op) {
  NodeIndex idx = mint_node_index();

  auto res = nodes_.emplace(idx, op);

  return res.second;
}

bool DataFlowGraph::AddEdge(std::shared_ptr<Operator> op1,
                            std::shared_ptr<Operator> op2) {
  EdgeIndex idx = mint_edge_index();
  Edge edge(op1, op2);

  auto res = edges_.emplace(idx, std::make_shared<Edge>(edge));

  return res.second;
}

std::vector<std::shared_ptr<Operator>> inputs() {
  // TODO get all input operators
  std::vector<std::shared_ptr<Operator>> v;
  return v;
}

}  // namespace dataflow
