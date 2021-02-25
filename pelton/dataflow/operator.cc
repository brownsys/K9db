#include "pelton/dataflow/operator.h"

#include "glog/logging.h"

namespace pelton {
namespace dataflow {

void Operator::AddParent(std::shared_ptr<Operator> parent,
                         std::shared_ptr<Edge> edge) {
  CHECK_EQ(edge->to().lock().get(), this);
  CHECK_EQ(edge->from(), parent);
  LOG(INFO) << "Adding edge: " << edge->from()->index() << " -> "
            << edge->to().lock()->index();
  this->parents_.push_back(edge);
  this->input_schemas_.push_back(parent->output_schema());
  parent->children_.emplace_back(edge);
  this->ComputeOutputSchema();
}

bool Operator::ProcessAndForward(NodeIndex source,
                                 const std::vector<Record> &records) {
  // Process the records generating the output vector.
  std::vector<Record> output;
  if (!this->Process(source, records, &output)) {
    return false;
  }

  // Pass output vector down to children to process.
  for (std::weak_ptr<Edge> edge_ptr : this->children_) {
    std::shared_ptr<Edge> edge = edge_ptr.lock();
    std::shared_ptr<Operator> child = edge->to().lock();
    if (!child->ProcessAndForward(this->index_, output)) {
      return false;
    }
  }

  return true;
}

std::vector<std::shared_ptr<Operator>> Operator::GetParents() const {
  std::vector<std::shared_ptr<Operator>> nodes;
  for (const auto &edge : this->parents_) {
    nodes.emplace_back(edge->from());
  }

  return nodes;
}

}  // namespace dataflow
}  // namespace pelton
