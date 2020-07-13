#ifndef PELTON_DATAFLOW_RECORD_H_
#define PELTON_DATAFLOW_RECORD_H_

#include <cstdint>

#include <limits>
#include <memory>
#include <vector>

#include <gtest/gtest_prod.h>

namespace dataflow {

class RecordData {
 public:
  RecordData(){};
  // TODO(malte): can we use generics here?
  explicit RecordData(std::unique_ptr<uint64_t> ptr);
  explicit RecordData(uint64_t val) { this->data_ = val; }

  RecordData ShallowClone() const;

  uintptr_t as_ptr() { return (uintptr_t)data_; }

  // TODO(malte): can we use generics here?
  uint64_t as_val() { return data_; }

 private:
  // 8 bytes of data, which represent either a pointer or an inline value.
  uint64_t data_;

  FRIEND_TEST(RecordTest, DataRep);
};

class Record {
 public:
  // TODO(malte): use move
  Record(bool positive, const std::vector<RecordData>& v,
         uint64_t inlining_bitmap)
      : data_inlining_bitmap_(inlining_bitmap),
        timestamp_(0),
        positive_(positive) {
    for (const RecordData& i : v) {
      data_.push_back(i.ShallowClone());
    }
  }

  Record() : timestamp_(0), positive_(true) {}
  Record(int size) : timestamp_(0), positive_(true) { data_.resize(size); }

  char* operator[](size_t index);
  char* at_mut(size_t index);
  const char* at(size_t index);

  bool positive() { return positive_; }
  void set_positive(bool pos) { positive_ = pos; };
  int timestamp() { return timestamp_; }
  void set_timestamp(int time) { timestamp_ = time; };

 private:
  // Data is interpreted according to schema stored outside the record.
  std::vector<RecordData> data_;
  // Bitmap indicating whether fields in data_ are pointers or inline values.
  // Starts at column 0 in LSB, column 64 in MSB.
  // TODO(malte): support >64 columns by using as pointer.
  // TODO(malte): consider moving this to a schema structure.
  uint64_t data_inlining_bitmap_;
  // Timestamp of this record.
  int timestamp_;
  // Is this a positive or negative record?
  bool positive_;

  bool is_inline(size_t index) {
    return data_inlining_bitmap_ & (0x1 << index);
  }

  FRIEND_TEST(RecordTest, DataRep);
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_RECORD_H_
