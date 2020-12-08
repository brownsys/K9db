#include "dataflow/record.h"

namespace dataflow {

void RecordData::DeleteOwnedData(uintptr_t ptr, DataType type) {
  switch (type) {
    case kText:
      delete reinterpret_cast<std::string*>(ptr);
      break;
    default:
      LOG(FATAL)
          << "Tried to delete inline data as if it was an owned pointer!";
  }
}

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

void Record::set_uint(size_t index, uint64_t v) {
  CheckType(index, DataType::kUInt);

  std::pair<bool, size_t> ri = schema_->RawColumnIndex(index);
  data_.inline_data_[ri.second] = v;
}

uint64_t Record::as_uint(size_t index) {
  CheckType(index, DataType::kUInt);

  std::pair<bool, size_t> ri = schema_->RawColumnIndex(index);
  return static_cast<uint64_t>(data_.inline_data_[ri.second]);
}

void Record::set_int(size_t index, int64_t v) {
  CheckType(index, DataType::kInt);

  std::pair<bool, size_t> ri = schema_->RawColumnIndex(index);
  data_.inline_data_[ri.second] = v;
}

int64_t Record::as_int(size_t index) {
  CheckType(index, DataType::kInt);

  std::pair<bool, size_t> ri = schema_->RawColumnIndex(index);
  return static_cast<int64_t>(data_.inline_data_[ri.second]);
}

void Record::set_string(size_t index, std::string* s) {
  CheckType(index, DataType::kText);

  std::pair<bool, size_t> ri = schema_->RawColumnIndex(index);
  data_.pointed_data_[ri.second] = reinterpret_cast<uintptr_t>(s);
}

std::string* Record::as_string(size_t index) {
  CheckType(index, DataType::kText);

  std::pair<bool, size_t> ri = schema_->RawColumnIndex(index);
  return reinterpret_cast<std::string*>(data_.pointed_data_[ri.second]);
}

// Index into record, either to the inline data or to the data pointed to by the
// pointer stored.
void* Record::operator[](size_t index) { return at_mut(index); }
const void* Record::operator[](size_t index) const { return at(index); }

}  // namespace dataflow
