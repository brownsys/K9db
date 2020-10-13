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

std::vector<std::shared_ptr<InputOperator>> DataFlowGraph::inputs() {
  std::vector<std::shared_ptr<InputOperator>> v;

  for (auto op : nodes_) {
    if (op.second->type() == OperatorType::INPUT) {
      v.emplace_back(std::static_pointer_cast<InputOperator>(op.second));
    }
  }

  return v;
}

std::vector<std::shared_ptr<MatViewOperator>> DataFlowGraph::outputs() {
  std::vector<std::shared_ptr<MatViewOperator>> v;

  for (auto op : nodes_) {
    if (op.second->type() == OperatorType::MAT_VIEW) {
      v.emplace_back(std::static_pointer_cast<MatViewOperator>(op.second));
    }
  }

  return v;
}

}  // namespace dataflow
