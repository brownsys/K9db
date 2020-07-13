#include "dataflow/record.h"

namespace dataflow {

RecordData::RecordData(std::unique_ptr<uint64_t> ptr) {
  uint64_t* raw_ptr = ptr.release();
  this->data_ = (uintptr_t)raw_ptr | TOP_BIT;
}

RecordData RecordData::ShallowClone() const {
  return RecordData {
    .data = this->data_,
  };
}

}  // namespace dataflow
