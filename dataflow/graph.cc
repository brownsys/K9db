#include "dataflow/graph.h"

#include <memory>

#include "glog/logging.h"

#include "dataflow/edge.h"

namespace dataflow {

DataFlowGraph::DataFlowGraph() {}

bool DataFlowGraph::AddInputNode(std::shared_ptr<InputOperator> op) {
  return AddNode(op, nullptr);
}

bool DataFlowGraph::AddNode(std::shared_ptr<Operator> op,
                            std::vector<std::shared_ptr<Operator>> parents) {
  NodeIndex idx = mint_node_index();
  op->set_index(idx);

  auto res = nodes_.emplace(idx, op);

  for(auto& parent : parents)
    if (parent != nullptr) {
      CHECK(AddEdge(parent, op));
    }

  return res.second;
}

bool DataFlowGraph::AddEdge(std::shared_ptr<Operator> parent,
                            std::shared_ptr<Operator> child) {
  EdgeIndex idx = mint_edge_index();
  std::shared_ptr<Edge> edge = std::make_shared<Edge>(Edge(parent, child));

  auto res = edges_.emplace(idx, edge);

  // also implicitly adds op1 as child of op2
  child->AddParent(parent, edge);

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

bool DataFlowGraph::Process(InputOperator& input, std::vector<Record> records) {
  return input.ProcessAndForward(records);
}

}  // namespace dataflow
