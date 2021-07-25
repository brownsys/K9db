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
  const std::optional<NodeIndex> &source_index() { return this->source_index_; }
  const std::vector<Record> &records() { return this->records_; }

 private:
  NodeIndex destination_index_;
  // If @source_index_ is null it implies that the records are meant for an
  // input operator and are being sent by the dataflow engine. Else they are
  // being sent by an exchange operator.
  std::optional<NodeIndex> source_index_;
  std::vector<Record> records_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_BATCH_MESSAGE_H_
