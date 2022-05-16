#include "pelton/dataflow/operator.h"

#include <algorithm>
#include <sstream>
#include <utility>

#include "glog/logging.h"
#include "pelton/dp/dp_util.h"

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
                                 std::vector<Record> &&records,
                                 Promise &&promise) {
  // Process the records generating the output vector.
  std::vector<Record> output =
      this->Process(source, std::move(records), promise);

  // Pass output vector down to children to process.
  this->BroadcastToChildren(std::move(output), std::move(promise), nullptr);
}

// A variant of ProcessAndForward which allows for the passing of a reference to a
// DPOptions. If the pointer is not null, then the record will be handled as part
// of a DP operation.
void Operator::ProcessAndForward(NodeIndex source,
                                 std::vector<Record> &&records,
                                 Promise &&promise,
                                 pelton::dp::DPOptions *dp_options = nullptr) {
  // Process the records generating the output vector.
  std::vector<Record> output;
  if ((dp_options != nullptr) &&
      (this->type() == pelton::dataflow::Operator::Type::AGGREGATE)) {
    // If there are DP options for this view and this is an aggregate operator, then use DP Processing
    output = this->ProcessDP(source, std::move(records), promise, dp_options);
  } else {
    output = this->Process(source, std::move(records), promise);
  }

  // Pass output vector down to children to process.
  this->BroadcastToChildren(std::move(output), std::move(promise), dp_options);
}

void Operator::BroadcastToChildren(
    std::vector<Record> &&records, Promise &&promise,
    pelton::dp::DPOptions *dp_options = nullptr) {
  if (this->children_.size() == 0 || records.size() == 0) {
    promise.Resolve();
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
    this->children_.at(i)->ProcessAndForward(this->index_, std::move(copy),
                                             promise.Derive(), dp_options);
  }
  // Reuse promise for the last child.
  this->children_.back()->ProcessAndForward(this->index_, std::move(records),
                                            std::move(promise), dp_options);
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
    for (const std::string &col : schema.column_names()) {
      str += "\"" + col + "\",";
    }
    str.pop_back();
    str += " ],";
  }
  str.pop_back();
  str += " ],\n";

  // print output schema
  str += "  \"output_columns\": [ ";
  for (const std::string &col : this->output_schema_.column_names()) {
    str += "\"" + col + "\",";
  }
  str.pop_back();
  str += " ],\n";
  return str;
}

Record Operator::DebugRecord() const {
  Record record{SchemaFactory::FLOW_DEBUG_SCHEMA, true};
  record.SetUInt(this->index_, 0);
  // Type.
  switch (this->type()) {
    case Operator::Type::INPUT:
      record.SetString(std::make_unique<std::string>("INPUT"), 1);
      break;
    case Operator::Type::IDENTITY:
      record.SetString(std::make_unique<std::string>("IDENTITY"), 1);
      break;
    case Operator::Type::MAT_VIEW:
      record.SetString(std::make_unique<std::string>("MAT_VIEW"), 1);
      break;
    case Operator::Type::FILTER:
      record.SetString(std::make_unique<std::string>("FILTER"), 1);
      break;
    case Operator::Type::UNION:
      record.SetString(std::make_unique<std::string>("UNION"), 1);
      break;
    case Operator::Type::EQUIJOIN:
      record.SetString(std::make_unique<std::string>("EQUIJOIN"), 1);
      break;
    case Operator::Type::PROJECT:
      record.SetString(std::make_unique<std::string>("PROJECT"), 1);
      break;
    case Operator::Type::AGGREGATE:
      record.SetString(std::make_unique<std::string>("AGGREGATE"), 1);
      break;
    case Operator::Type::EXCHANGE:
      record.SetString(std::make_unique<std::string>("EXCHANGE"), 1);
      break;
  }
  // Output schema.
  std::stringstream out_buffer;
  out_buffer << " (" << this->output_schema_ << ") ";
  record.SetString(std::make_unique<std::string>(out_buffer.str()), 2);
  // Children.
  std::string children = "[";
  for (Operator *child : this->children_) {
    children += std::to_string(child->index_) + ",";
  }
  if (this->children_.size() > 0) {
    children.pop_back();
  }
  children += "]";
  record.SetString(std::make_unique<std::string>(children), 3);
  // Parents.
  std::string parents = "[";
  for (Operator *parent : this->parents_) {
    parents += std::to_string(parent->index_) + ",";
  }
  if (this->parents_.size() > 0) {
    parents.pop_back();
  }
  parents += "]";
  record.SetString(std::make_unique<std::string>(parents), 4);
  // No info unless overriden.
  record.SetString(std::make_unique<std::string>(""), 5);
  return record;
}

uint64_t Operator::SizeInMemory(const std::string &flow_name,
                                std::vector<Record> *output) const {
  uint64_t size = this->SizeInMemory() / 1024;
  std::string id = std::to_string(this->index_);
  // Combine size into this operator record (from other partitions).
  bool found = false;
  for (size_t i = 0; i < output->size(); i++) {
    Record &r = output->at(i);
    if (r.GetString(0) == flow_name && r.GetString(1) == id) {
      found = true;
      r.SetUInt(r.GetUInt(2) + size, 2);
      break;
    }
  }
  if (!found) {
    output->emplace_back(SchemaFactory::MEMORY_SIZE_SCHEMA, true,
                         std::make_unique<std::string>(flow_name),
                         std::make_unique<std::string>(id), size);
  }
  return size;
}

}  // namespace dataflow
}  // namespace pelton
