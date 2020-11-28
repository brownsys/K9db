#ifndef PELTON_DATAFLOW_KEY_H_
#define PELTON_DATAFLOW_KEY_H_

#include "dataflow/schema.h"

namespace dataflow {

class Key {
 public:
  Key(const void* field, DataType type)
      : data_(reinterpret_cast<uint64_t>(field)), type_(type) {}

  template <typename H>
  friend H AbslHashValue(H h, const Key& k) {
    if (Schema::is_inlineable(k.type_)) {
      return H::combine(std::move(h), k.data_);
    } else {
      // XXX(malte): fix!
      return H::combine(std::move(h), k.data_);
    }
  }

 private:
  uint64_t data_;
  DataType type_;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_KEY_H_
