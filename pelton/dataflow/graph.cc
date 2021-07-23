#include "pelton/dataflow/graph.h"

#include <memory>

#include "glog/logging.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"

namespace pelton {
namespace dataflow {

bool DataFlowGraph::AddInputNode(std::shared_ptr<InputOperator> op) {
  CHECK(this->inputs_.count(op->input_name()) == 0)
      << "An operator for this input already exists";
  this->inputs_.emplace(op->input_name(), op);
  return this->AddNode(op, std::vector<std::shared_ptr<Operator>>{});
}

bool DataFlowGraph::AddOutputOperator(std::shared_ptr<MatViewOperator> op,
                                      std::shared_ptr<Operator> parent) {
  this->outputs_.emplace_back(op);
  return this->AddNode(op, parent);
}

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
  std::pair<NodeIndex, NodeIndex> edge{parent->index(), child->index()};
  this->edges_.push_back(edge);
  // Also implicitly adds op1 as child of op2.
  child->AddParent(parent, edge);
  return true;
}

bool DataFlowGraph::Process(const std::string &input_name,
                            const std::vector<Record> &records) {
  std::shared_ptr<InputOperator> node = this->inputs_.at(input_name);
  if (!node->ProcessAndForward(UNDEFINED_NODE_INDEX, records)) {
    return false;
  }

  return true;
}

std::shared_ptr<DataFlowGraph> DataFlowGraph::Clone() {
  auto clone = std::make_shared<DataFlowGraph>();
  for (auto const &item : this->inputs_) {
    auto input_clone =
        std::static_pointer_cast<InputOperator>(item.second->Clone());
    input_clone->graph_ = clone.get();
    clone->inputs_.emplace(item.first, input_clone);
  }
  for (std::shared_ptr<MatViewOperator> const &matview : this->outputs_) {
    auto matview_clone =
        std::static_pointer_cast<MatViewOperator>(matview->Clone());
    matview_clone->graph_ = clone.get();
    clone->outputs_.push_back(matview_clone);
    clone->nodes_.emplace(matview_clone->index_, matview_clone);
  }
  // The cloned nodes have their indices set during the operator cloning itself
  for (size_t i = 0; i < this->nodes_.size(); i++) {
    auto node_clone = this->nodes_.at(i)->Clone();
    assert(node_clone->index_ == i);
    node_clone->graph_ = clone.get();
    clone->nodes_.emplace(i, node_clone);
  }
  // Edges can be trivially copied
  clone->edges_ = this->edges_;
  return clone;
}

std::string DataFlowGraph::DebugString() const {
  std::string str = "[";
  for (const auto &[_, node] : this->nodes_) {
    str += "\n\t" + node->DebugString() + ",";
  }
  str.pop_back();
  str += "\n]";
  return str;
}

uint64_t DataFlowGraph::SizeInMemory() const {
  uint64_t size = 0;
  for (const auto &[_, node] : this->nodes_) {
    size += node->SizeInMemory();
  }
  return size;
}

}  // namespace dataflow
}  // namespace pelton
