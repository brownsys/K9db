#ifndef PELTON_DATAFLOW_STOP_MESSAGE_H_
#define PELTON_DATAFLOW_STOP_MESSAGE_H_

#include <string>
#include <vector>

#include "pelton/dataflow/message.h"

namespace pelton {
namespace dataflow {

class StopMessage : public Message {
 public:
  StopMessage() : Message(Message::Type::STOP) {}
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_STOP_MESSAGE_H_
