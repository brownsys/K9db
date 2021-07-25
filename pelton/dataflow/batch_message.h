#ifndef PELTON_DATAFLOW_BATCH_MESSAGE_H_
#define PELTON_DATAFLOW_BATCH_MESSAGE_H_

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "pelton/dataflow/message.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class BatchMessage : public Message {
 public:
  BatchMessage(NodeIndex destination_index, std::vector<Record> records)
      : Message(Message::Type::BATCH),
        destination_index_(destination_index),
        source_index_(std::nullopt),
        records_(std::move(records)) {}
  BatchMessage(NodeIndex destination_index, NodeIndex source_index,
               std::vector<Record> records)
      : Message(Message::Type::BATCH),
        destination_index_(destination_index),
        source_index_(source_index),
        records_(std::move(records)) {}

  std::vector<Record> ConsumeRecords() { return std::move(this->records_); }

  // Accessors
  const NodeIndex &destination_index() { return this->destination_index_; }
  const NodeIndex &source_index() {
    assert(this->source_index_);
    return this->source_index_.value();
  }
  const std::vector<Record> &records() { return this->records_; }

 private:
  NodeIndex destination_index_;
  std::optional<NodeIndex> source_index_;
  std::vector<Record> records_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_BATCH_MESSAGE_H_
