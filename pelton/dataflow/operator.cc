#include "pelton/dataflow/operator.h"

#include "glog/logging.h"

namespace pelton {
namespace dataflow {

void Operator::AddParent(std::shared_ptr<Operator> parent,
                         std::shared_ptr<Edge> edge) {
  CHECK_EQ(edge->to().lock().get(), this);
  CHECK_EQ(edge->from(), parent);
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

std::string Operator::DebugString() const {
  std::string type_str = "";
  switch (this->type()) {
    case Operator::Type::INPUT:
      type_str = "INPUT";
      break;
    case Operator::Type::IDENTITY:
      type_str = "IDENTITY";
      break;
    case Operator::Type::MAT_VIEW:
      type_str = "MAT_VIEW";
      break;
    case Operator::Type::FILTER:
      type_str = "FILTER";
      break;
    case Operator::Type::UNION:
      type_str = "UNION";
      break;
    case Operator::Type::EQUIJOIN:
      type_str = "EQUIJOIN";
      break;
    case Operator::Type::PROJECT:
      type_str = "PROJECT";
      break;
    case Operator::Type::AGGREGATE:
      type_str = "AGGREGATE";
      break;
  }

  std::string str = "{";
  str += "\"operator\": \"" + type_str + "\", ";
  str += "\"id\": " + std::to_string(this->index()) + ", ";
  str += "\"children\": [";
  for (const std::weak_ptr<Edge> &edge : this->children_) {
    str += std::to_string(edge.lock()->to().lock()->index()) + ",";
  }
  if (this->children_.size() > 0) {
    str.pop_back();
  }
  str += "]";
  str += "}";
  return str;
}

}  // namespace dataflow
}  // namespace pelton
