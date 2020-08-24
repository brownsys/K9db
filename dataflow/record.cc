#include "dataflow/record.h"

namespace dataflow {

RecordData::RecordData(std::unique_ptr<uint64_t> ptr) {
  uint64_t* raw_ptr = ptr.release();
  data_ = (uintptr_t)raw_ptr;
}

RecordData RecordData::ShallowClone() const {
  RecordData d;
  d.data_ = data_;
  return d;
}

// Index into record, either to the inline data or to the data pointed to by the
// pointer stored.
const char* Record::operator[](size_t index) const { return at(index); }
char* Record::operator[](size_t index) { return at_mut(index); }

char* Record::at_mut(size_t index) {
  if (is_inline(index)) {
    return (char*)data_[index].raw_data_ptr();
  } else {
    return (char*)data_[index].as_ptr();
  }
}

const char* Record::at(size_t index) const {
  if (is_inline(index)) {
    return (const char*)data_[index].raw_const_data_ptr();
  } else {
    return (const char*)data_[index].as_const_ptr();
  }
}

}  // namespace dataflow
