#ifndef PELTON_DATAFLOW_BATCH_MESSAGE_H_
#define PELTON_DATAFLOW_BATCH_MESSAGE_H_

#include <optional>
#include <string>
#include <vector>

#include "pelton/dataflow/message.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class BatchMessage : public Message {
 public:
  BatchMessage(std::string input_name, std::vector<Record> records)
      : Message(Message::Type::BATCH),
        input_name_(input_name),
        entry_index_(std::nullopt),
        source_index_(std::nullopt),
        records_(std::move(records)) {}
  BatchMessage(NodeIndex entry_index, NodeIndex source_index,
               std::vector<Record> records)
      : Message(Message::Type::BATCH),
        input_name_(std::nullopt),
        entry_index_(entry_index),
        source_index_(source_index),
        records_(std::move(records)) {}

  std::vector<Record> ConsumeRecords() { return std::move(this->records_); }
  bool ContainsInput() { return this->input_name_.has_value(); }

  // Accessors
  const std::string &input_name() {
    assert(this->input_name_);
    return this->input_name_.value();
  }
  const NodeIndex &entry_index() {
    assert(this->entry_index_);
    return this->entry_index_.value();
  }
  const NodeIndex &source_index() {
    assert(this->source_index_);
    return this->source_index_.value();
  }
  const std::vector<Record> &records() { return this->records_; }

 private:
  std::optional<std::string> input_name_;
  std::optional<NodeIndex> entry_index_;
  std::optional<NodeIndex> source_index_;
  std::vector<Record> records_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_BATCH_MESSAGE_H_
