#ifndef PELTON_DATAFLOW_RECORD_H_
#define PELTON_DATAFLOW_RECORD_H_

#include <cstdint>

#include <limits>
#include <memory>
#include <vector>

#include <gtest/gtest_prod.h>

#define TOP_BIT std::numeric_limits<int64_t>::min()

namespace dataflow {

class RecordData {
 public:
  RecordData(){};
  // TODO(malte): can we use generics here?
  explicit RecordData(std::unique_ptr<uint64_t> ptr);
  explicit RecordData(uint64_t val) { this->data_ = val; }

  // Dereference to a byte-aligned pointer, either to the inline data or to the
  // data pointed to by the pointer stored.
  char* operator*() {
    if (data_ & TOP_BIT) {
      return (char*)(data_ ^ TOP_BIT);
    } else {
      return (char*)&data_;
    }
  }

  RecordData ShallowClone() const;

 private:
  // Store 8 bytes, which are either an immediate value or a pointer to data,
  // signified by the top bit (which isn't valid in an x86-64 pointer).
  //
  // 1) INLINE DATA FORMAT
  //
  //  63                                 ...       0
  // +-----------------------------------...--------+
  // | Inline data (64b)                 ...        |
  // +-----------------------------------...--------+
  //
  // 2) POINTER FORMAT
  //
  //  63           62  ...   48 47       ...       0
  // +------------+----...-----+---------...--------+
  // | Immed. bit | UNUSED     | Pointer addr (48b) |
  // +------------+----...-----+---------...--------+
  uint64_t data_;

  FRIEND_TEST(RecordTest, DataRep);
};

class Record {
 private:
  bool positive_;
  int timestamp_;

  // data is interpreted according to schema stored outside the record.
  std::vector<RecordData> data_;

 public:
  Record(bool positive, const std::vector<RecordData>& v)
      : positive_(positive), timestamp_(0) {
    for (const RecordData& i : v) {
      data_.push_back(i.ShallowClone());
    }
  }

  Record() : positive_(true), timestamp_(0) {}

  Record(int size) : positive_(true), timestamp_(0) { data_.resize(size); }

  bool is_positive() { return positive_; }

  int timestamp() { return timestamp_; }
  void set_timestamp(int time) { timestamp_ = time; };
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_RECORD_H_
