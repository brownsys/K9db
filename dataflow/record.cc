#include "dataflow/record.h"

namespace dataflow {

RecordData::RecordData(std::unique_ptr<uint64_t> ptr) {
  uint64_t* raw_ptr = ptr.release();
  data_ = (uintptr_t)raw_ptr;
}

RecordData RecordData::ShallowClone() const {
  return RecordData{
      .data = data_,
  };
}

// Index into record, either to the inline data or to the data pointed to by the
// pointer stored.
char* Record::operator[](size_t index) { return at_mut(index); }

char* Record::at_mut(size_t index) {
  if (is_inline(index)) {
    return (char*)data_[index].as_val();
  } else {
    return (char*)data_[index].as_ptr();
  }
}

const char* Record::at(size_t index) { return (const char*)at_mut(index); }

}  // namespace dataflow
