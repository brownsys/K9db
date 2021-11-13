#include "pelton/dataflow/operator.h"

#include <algorithm>
#include <utility>

#include "glog/logging.h"

namespace pelton {
namespace dataflow {

void Operator::AddParent(Operator *parent) {
  this->parents_.push_back(parent);
  // Project operator's schema should be computed after operations have been
  // added.
  if (parent->type() == Operator::Type::PROJECT) {
    auto old = parent->output_schema_;
    parent->ComputeOutputSchema();
    if (old) {
      CHECK_EQ(parent->output_schema_, old);
    }
  }
  this->input_schemas_.push_back(parent->output_schema());
  parent->children_.push_back(this);
  if (this->type() != Operator::Type::PROJECT) {
    auto old = this->output_schema_;
    this->ComputeOutputSchema();
    if (old) {
      CHECK_EQ(this->output_schema_, old);
    }
  }
}
void Operator::AddParentAt(Operator *parent, size_t parent_index) {
  CHECK_LE(parent_index, this->parents_.size());
  if (parent_index < this->parents_.size()) {
    this->parents_.insert(this->parents_.begin() + parent_index, parent);
  } else {
    this->parents_.push_back(parent);
  }

  // Project operator's schema should be computed after operations have been
  // added.
  if (parent->type() == Operator::Type::PROJECT) {
    auto old = parent->output_schema_;
    parent->ComputeOutputSchema();
    if (old) {
      CHECK_EQ(parent->output_schema_, old);
    }
  }

  // Push the input schema to the right place.
  if (parent_index < this->parents_.size()) {
    this->input_schemas_.insert(this->input_schemas_.begin() + parent_index,
                                parent->output_schema());
  } else {
    this->input_schemas_.push_back(parent->output_schema());
  }

  parent->children_.push_back(this);
  if (this->type() != Operator::Type::PROJECT) {
    auto old = this->output_schema_;
    this->ComputeOutputSchema();
    if (old) {
      CHECK_EQ(this->output_schema_, old);
    }
  }
}
size_t Operator::RemoveParent(Operator *parent) {
  // Remove parent from this.
  size_t parent_index = UNDEFINED_NODE_INDEX;
  for (size_t i = 0; i < this->parents_.size(); i++) {
    if (this->parents_.at(i) == parent) {
      this->parents_.erase(this->parents_.begin() + i);
      this->input_schemas_.erase(this->input_schemas_.begin() + i);
      parent_index = i;
    }
  }
  CHECK_NE(parent_index, UNDEFINED_NODE_INDEX);
  // Remove this from parent.
  auto it = std::find(parent->children_.begin(), parent->children_.end(), this);
  parent->children_.erase(it);
  return parent_index;
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
    this->children_.at(i)->ProcessAndForward(this->index_, std::move(copy));
  }
  this->children_.back()->ProcessAndForward(this->index_, std::move(records));
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
    case Operator::Type::EXCHANGE:
      type_str = "EXCHANGE";
      break;
  }

  std::string str = "";
  str += "\"operator\": \"" + type_str + "\", ";
  str += "\"id\": " + std::to_string(this->index()) + ", ";
  str += "\"children\": [ ";
  for (Operator *child : this->children_) {
    str += std::to_string(child->index()) + ",";
  }
  str.pop_back();
  str += " ],\n";

  // print input schema
  str += "  \"input_columns\": [ ";
  for (const SchemaRef schema : this->input_schemas_) {
    str += "[ ";
    for (const std::string col : schema.column_names()) {
      str += "\"" + col + "\",";
    }
    str.pop_back();
    str += " ],";
  }
  str.pop_back();
  str += " ],\n";

  // print output schema
  str += "  \"output_columns\": [ ";
  for (const std::string col : this->output_schema_.column_names()) {
    str += "\"" + col + "\",";
  }
  str.pop_back();
  str += " ],\n";
  return str;
}

}  // namespace dataflow
}  // namespace pelton
