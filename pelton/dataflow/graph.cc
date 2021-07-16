#include "pelton/dataflow/graph.h"

#include <memory>

#include "glog/logging.h"
#include "pelton/dataflow/batch_message.h"
#include "pelton/dataflow/channel.h"
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

bool DataFlowGraph::InsertNodeAfter(std::shared_ptr<Operator> op,
                                    std::shared_ptr<Operator> parent) {
  // Clear children for @parent
  std::vector<NodeIndex> old_children = parent->children_;
  parent->children_ = std::vector<NodeIndex>{};
  // Add new node to the graph
  this->AddNode(op, parent);

  // Add children for @op
  op->children_ = old_children;
  // Replace @parent with @op as parent for the above children
  for (auto child_index : op->children_) {
    auto child_op = this->GetNode(child_index);
    for (size_t i = 0; i < child_op->parents_.size(); i++) {
      if (child_op->parents_.at(i) == parent->index()) {
        child_op->parents_.at(i) = op->index();
      }
    }
  }
  // Update stale edges
  for (auto &edge : this->edges_) {
    for (auto child_index : old_children) {
      if (edge.first == parent->index() && edge.second == child_index) {
        edge.first = op->index();
      }
    }
  }

  return true;
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

void DataFlowGraph::Start(std::shared_ptr<Channel> incoming_chan) const {
  while (true) {
    std::vector<std::shared_ptr<Message>> messages = incoming_chan->Recv();
    for (auto msg : messages) {
      switch (msg->type()) {
        case Message::Type::BATCH:
          LOG(INFO) << "Recieved batch with";
          break;
        case Message::Type::STOP:
          // Terminate this thread
          return;
      }
    }
  }
}

std::string DataFlowGraph::DebugString() const {
  std::string str = "[";
  for (int i = (this->nodes_.size() - 1); i >= 0; i--) {
    str += "\n\t" + this->nodes_.at(i)->DebugString() + ",";
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
