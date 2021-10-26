#include "pelton/dataflow/operator.h"

#include <utility>

#include "glog/logging.h"
#include "pelton/dataflow/graph.h"

namespace pelton {
namespace dataflow {

void Operator::AddParent(std::shared_ptr<Operator> parent) {
  this->parents_.push_back(parent->index());
  // Project operator's schema should be computed after operations have been
  // added.
  if (parent->type() == Operator::Type::PROJECT) {
    parent->ComputeOutputSchema();
  }
  this->input_schemas_.push_back(parent->output_schema());
  parent->children_.push_back(this->index());
  if (this->type() != Operator::Type::PROJECT) {
    this->ComputeOutputSchema();
  }
}

void Operator::ProcessAndForward(NodeIndex source,
                                 std::vector<Record> &&records) {
  // Process the records generating the output vector.
  std::vector<Record> output = this->Process(source, std::move(records));
  // Pass output vector down to children to process.
  if (output.size() > 0) {
    this->BroadcastToChildren(std::move(output));
  }
}

void Operator::BroadcastToChildren(std::vector<Record> &&records) {
  if (this->children_.size() == 0) {
    return;
  }
  // We are at a fork in the graph with many children.
  for (size_t i = 0; i < this->children_.size() - 1; i++) {
    // We need to copy the records for each child (except the last one).
    std::vector<Record> copy;
    copy.reserve(records.size());
    for (const Record &r : records) {
      copy.push_back(r.Copy());
    }
    // Move the copy to child.
    this->partition_->GetNode(this->children_.at(i))
        ->ProcessAndForward(this->index_, std::move(copy));
  }
  this->partition_->GetNode(this->children_.back())
      ->ProcessAndForward(this->index_, std::move(records));
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
    str.pop_back();
  }
  str += "],\n";

  // print input schema
  str += "  \"input_columns\": [ ";
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
  str += " ],\n";

  // print output schema
  str += "  \"output_columns\": [";
  for (const std::string col : this->output_schema_.column_names()) {
    str += "\"" + col + "\", ";
  }
  str.pop_back();
  str.pop_back();
  str += "],\n";
  return str;
}

void Operator::CloneInto(std::shared_ptr<Operator> clone) const {
  clone->children_ = this->children_;
  clone->parents_ = this->parents_;
  clone->input_schemas_ = this->input_schemas_;
  clone->output_schema_ = this->output_schema_;
}

}  // namespace dataflow
}  // namespace pelton
