#ifndef PELTON_DATAFLOW_RECORD_H_
#define PELTON_DATAFLOW_RECORD_H_

#include <cstdint>

#include <cassert>
#include <limits>
#include <memory>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest_prod.h"

namespace dataflow {

typedef uint32_t ColumnID;

// `RecordData` encapsulates either a pointer to external data, or an inline
// value. To avoid per-record space overhead, this type does *not* include
// any indication of whether the data is inlined or not; it is up to the
// caller to choose the right interpretation. Typically, `RecordData` is
// inlined in a `Record` type, which maintains a bitmap of inlining states.
class RecordData {
 public:
  RecordData(){};
  // TODO(malte): can we use generics here?
  explicit RecordData(std::unique_ptr<uint64_t> ptr);
  explicit RecordData(uint64_t val) { data_ = val; }

  RecordData ShallowClone() const;

  uintptr_t as_ptr() { return (uintptr_t)data_; }
  const uintptr_t as_const_ptr() const { return (const uintptr_t)data_; }

  // TODO(malte): can we use generics here?
  uint64_t as_val() { return data_; }

  // TODO(malte): needed so that Record can access data_, but ideally
  // we'd solve this differently
  uintptr_t raw_data_ptr() { return (uintptr_t)&data_; }
  const uintptr_t raw_const_data_ptr() const { return (uintptr_t)&data_; }

  bool operator==(const RecordData& other) const {
    return this->data_ == other.data_;
  }

  // XXX(malte): for pointer-valued data, this hashes the *pointer*, not
  // the data!
  template <typename H>
  friend H AbslHashValue(H h, const RecordData& rd) {
    return H::combine(std::move(h), rd.data_);
  }

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
  explicit Record(int size) : timestamp_(0), positive_(true) {
    data_.resize(size);
  }

  char* operator[](size_t index);
  const char* operator[](size_t index) const;
  char* at_mut(size_t index);
  const char* at(size_t index) const;
  RecordData raw_at(size_t index) const {
    CHECK_LT(index, data_.size());
    return data_[index];
  }

  bool positive() const { return positive_; }
  void set_positive(bool pos) { positive_ = pos; };
  int timestamp() const { return timestamp_; }
  void set_timestamp(int time) { timestamp_ = time; };

  // XXX(malte): this does not do a deep compare; i.e., it compares
  // pointers for non-inlined values
  bool operator==(const Record& other) const {
    uint64_t mask = 0;
    for (size_t i = 0; i < data_.size(); ++i) {
      mask |= (1 << i);
    }
    if (data_inlining_bitmap_ == mask) {
      // all columns are inline values, so we can compare directly
      return this->data_ == other.data_;
    } else {
      // some columns are not inlined, so we must do a deep compare
      // TODO(malte): implement
      assert(false);
    }
  }

 private:
  // Data is interpreted according to schema stored outside the record.
  // [24 B]
  std::vector<RecordData> data_;
  // Bitmap indicating whether fields in data_ are pointers or inline values.
  // Starts at column 0 in LSB, column 64 in MSB.
  // TODO(malte): support >64 columns by using as pointer.
  // TODO(malte): consider moving this to a schema structure.
  // [8 B]
  uint64_t data_inlining_bitmap_;
  // Timestamp of this record.
  // [4 B]
  int timestamp_;
  // Is this a positive or negative record?
  // [1 B]
  bool positive_;

  // *******
  // 11B SPARE on x86-64 due to alignment
  // *******

  bool is_inline(size_t index) const {
    return data_inlining_bitmap_ & (0x1 << index);
  }

  FRIEND_TEST(RecordTest, DataRep);
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_RECORD_H_
