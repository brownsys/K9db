#ifndef PELTON_DATAFLOW_MESSAGE_H_
#define PELTON_DATAFLOW_MESSAGE_H_

#include <string>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class Message {
 public:
  enum class Type {
    BATCH,
    STOP,
  };

  // Cannot copy a messsage.
  Message(const Message &other) = delete;
  Message &operator=(const Message &other) = delete;

  Type type() const { return this->type_; }

  virtual ~Message() = default;

 protected:
  explicit Message(Type type) : type_(type) {}

 private:
  Type type_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_MESSAGE_H_
