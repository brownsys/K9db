#include "pelton/dataflow/operator.h"

#include "glog/logging.h"
#include "pelton/dataflow/graph.h"

namespace pelton {
namespace dataflow {

void Operator::AddParent(std::shared_ptr<Operator> parent,
                         std::tuple<NodeIndex, NodeIndex> edge) {
  CHECK_EQ(this->index(), std::get<1>(edge));
  this->parents_.push_back(std::get<0>(edge));
  // Project operator's schema should be computed after operations have been
  // added.
  if (parent->type() == Operator::Type::PROJECT) {
    parent->ComputeOutputSchema();
  }
  this->input_schemas_.push_back(parent->output_schema());
  parent->children_.emplace_back(std::get<1>(edge));
  if (this->type() != Operator::Type::PROJECT) {
    this->ComputeOutputSchema();
  }
}

bool Operator::ProcessAndForward(NodeIndex source,
                                 const std::vector<Record> &records) {
  // Process the records generating the output vector.
  std::vector<Record> output;
  if (!this->Process(source, records, &output)) {
    return false;
  }

  // Pass output vector down to children to process.
  for (NodeIndex child_index : this->children_) {
    std::shared_ptr childNode = this->graph()->GetNode(child_index);
    if (!childNode->ProcessAndForward(this->index_, output)) {
      return false;
    }
  }

  return true;
}

std::vector<std::shared_ptr<Operator>> Operator::GetParents() const {
  std::vector<std::shared_ptr<Operator>> nodes;
  for (NodeIndex parent_index : this->parents_) {
    nodes.emplace_back(this->graph()->GetNode(parent_index));
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
  for (NodeIndex child_index : this->children_) {
    str += std::to_string(child_index) + ",";
  }
  if (this->children_.size() > 0) {
    str.pop_back();
  }
  str += "], ";

  // print input schema
  str += "\"input_columns\": [";
  for (const SchemaRef schema : this->input_schemas_) {
    str += "[";
    for (const std::string col : schema.column_names()) {
      str += "\"" + col + "\", ";
    }
    str.pop_back();
    str.pop_back();
    str += "], ";
  }
  str.pop_back();
  str.pop_back();
  str += "], ";

  // print output schema
  str += "\"output_columns\": [";
  for (const std::string col : this->output_schema_.column_names()) {
    str += "\"" + col + "\", ";
  }
  str.pop_back();
  str.pop_back();
  str += "]";

  str += "}\n";
  return str;
}

}  // namespace dataflow
}  // namespace pelton
