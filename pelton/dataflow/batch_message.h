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
  explicit BatchMessage(std::vector<Record> records)
      : Message(Message::Type::BATCH), records_(std::move(records)) {}

  std::vector<Record> ConsumeRecords() { return std::move(this->records_); }
  // Accessors
  const std::vector<Record> &records() { return this->records_; }

 private:
  std::vector<Record> records_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_BATCH_MESSAGE_H_
