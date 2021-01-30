
#include "glog/logging.h"

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

size_t Record::size_at(size_t index) const {
  const uintptr_t* data = static_cast<const uintptr_t*>(at(index));
  return schema_->size_of(schema_->TypeOf(index),
                          reinterpret_cast<const void*>(*data));
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

uint64_t Record::as_uint(size_t index) const {
  CheckType(index, DataType::kUInt);

  std::pair<bool, size_t> ri = schema_->RawColumnIndex(index);
  return static_cast<uint64_t>(data_.inline_data_[ri.second]);
}

void Record::set_int(size_t index, int64_t v) {
  CheckType(index, DataType::kInt);

  std::pair<bool, size_t> ri = schema_->RawColumnIndex(index);
  data_.inline_data_[ri.second] = v;
}

int64_t Record::as_int(size_t index) const {
  CheckType(index, DataType::kInt);

  std::pair<bool, size_t> ri = schema_->RawColumnIndex(index);
  return static_cast<int64_t>(data_.inline_data_[ri.second]);
}

void Record::set_string(size_t index, std::string* s) {
  CheckType(index, DataType::kText);

  std::pair<bool, size_t> ri = schema_->RawColumnIndex(index);
  data_.pointed_data_[ri.second] = reinterpret_cast<uintptr_t>(s);
}

std::string* Record::as_string(size_t index) const {
  CheckType(index, DataType::kText);

  std::pair<bool, size_t> ri = schema_->RawColumnIndex(index);
  return reinterpret_cast<std::string*>(data_.pointed_data_[ri.second]);
}

const std::pair<Key, bool> Record::GetKey() const {
  std::vector<ColumnID> key_cols = schema_->key_columns();
  CHECK_EQ(key_cols.size(), 1);
  ColumnID cid = key_cols[0];

  bool is_inline = Schema::is_inlineable(schema_->TypeOf(cid));
  if (is_inline) {
    const uint64_t key = *reinterpret_cast<const uint64_t*>(at(cid));
    return std::make_pair(Key(key, schema_->TypeOf(cid)), is_inline);
  } else {
    const void* key = at(cid);
    return std::make_pair(Key(key, schema_->TypeOf(cid)), is_inline);
  }
}

// Index into record, either to the inline data or to the data pointed to by the
// pointer stored.
void* Record::operator[](size_t index) { return at_mut(index); }
const void* Record::operator[](size_t index) const { return at(index); }

}  // namespace dataflow
