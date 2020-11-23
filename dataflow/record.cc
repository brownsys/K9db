#include "dataflow/record.h"

namespace dataflow {

const void* Record::at(size_t index) const {
  std::pair<bool, size_t> ri = schema_->RawColumnIndex(index);
  if (ri.first) {
    return &data_.inline_data_[ri.second];
  } else {
    return static_cast<void*>(&data_.pointed_data_[ri.second]);
  }
}

void* Record::at_mut(size_t index) {
  std::pair<bool, size_t> ri = schema_->RawColumnIndex(index);
  if (ri.first) {
    return &data_.inline_data_[ri.second];
  } else {
    return static_cast<void*>(&data_.pointed_data_[ri.second]);
  }
}

// Index into record, either to the inline data or to the data pointed to by the
// pointer stored.
void* Record::operator[](size_t index) { return at_mut(index); }
const void* Record::operator[](size_t index) const { return at(index); }

}  // namespace dataflow
