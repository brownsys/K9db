#include <algorithm>

#include "glog/logging.h"

#include "dataflow/operator.h"

namespace dataflow {

void Operator::AddParent(std::shared_ptr<Operator> parent,
                         std::shared_ptr<Edge> edge) {
  LOG(INFO) << "Adding edge: " << edge->from() << " -> " << edge->to();
  parents_.push_back(edge);
  parent->children_.push_back(edge);
}

bool Operator::ProcessAndForward(std::vector<Record>& rs) {
  std::vector<Record> out;

  if (!process(rs, out)) {
    return false;
  }

  for (auto edge : children_) {
    auto child = edge->to();
    auto fwd_rs = out;
    if (!child->ProcessAndForward(fwd_rs)) {
      return false;
    }
  }

  return true;
}

}  // namespace dataflow

