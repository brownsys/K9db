#include <algorithm>

#include "glog/logging.h"

#include "dataflow/operator.h"

namespace dataflow {

void Operator::AddParent(std::shared_ptr<Operator> parent,
                         std::shared_ptr<Edge> edge) {
  LOG(INFO) << "Adding edge: " << edge->from()->index() << " -> "
            << edge->to().lock()->index();
  parents_.push_back(edge);
  parent->children_.push_back(std::weak_ptr<Edge>(edge));
}

bool Operator::ProcessAndForward(std::vector<Record>& rs) {
  std::vector<Record> out;

  if (!process(rs, out)) {
    return false;
  }

  for (std::weak_ptr<Edge> edge_ptr : children_) {
    std::shared_ptr<Edge> edge = edge_ptr.lock();
    if (edge) {
      auto child = edge->to();
      if (!child.lock()->ProcessAndForward(out)) {
        return false;
      }
    }
  }

  return true;
}

std::vector<std::shared_ptr<Operator>> Operator::parents() const {
  // lookup from graph
  assert(graph_);

  std::vector<std::shared_ptr<Operator>> nodes;
  for(const auto& edge : parents_) {
    CHECK_EQ(edge->to().lock().get(), this);
    nodes.emplace_back(edge->from());
  }

  return nodes;
}

}  // namespace dataflow
