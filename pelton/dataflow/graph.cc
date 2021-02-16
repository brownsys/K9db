#include "pelton/dataflow/graph.h"

#include <memory>

#include "glog/logging.h"
#include "pelton/dataflow/edge.h"

namespace pelton {
namespace dataflow {

bool DataFlowGraph::AddNode(std::shared_ptr<Operator> op,
                            std::vector<std::shared_ptr<Operator>> parents) {
  NodeIndex idx = this->MintNodeIndex();
  op->SetIndex(idx);
  op->SetGraph(this);

  const auto &[_, inserted] = this->nodes_.emplace(idx, op);
  CHECK(inserted);

  for (const auto &parent : parents) {
    if (parent) {
      CHECK(this->AddEdge(parent, op));
    }
  }
  return inserted;
}

bool DataFlowGraph::AddEdge(std::shared_ptr<Operator> parent,
                            std::shared_ptr<Operator> child) {
  EdgeIndex idx = this->MintEdgeIndex();
  std::shared_ptr<Edge> edge = std::make_shared<Edge>(parent, child);

  const auto &[_, inserted] = this->edges_.emplace(idx, edge);
  CHECK(inserted);

  // Also implicitly adds op1 as child of op2.
  child->AddParent(parent, edge);

  return inserted;
}

bool DataFlowGraph::Process(const std::string &input_name,
                            const std::vector<Record> &records) {
  for (auto &node : this->inputs_) {
    if (node->input_name() == input_name) {
      if (!node->ProcessAndForward(UNDEFINED_NODE_INDEX, records)) {
        return false;
      }
    }
  }

  return true;
}

}  // namespace dataflow
}  // namespace pelton
