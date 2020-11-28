#ifndef PELTON_DATAFLOW_RECORD_H_
#define PELTON_DATAFLOW_RECORD_H_

#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest_prod.h"

#include "dataflow/schema.h"
#include "dataflow/types.h"

namespace dataflow {

class RecordData {
 public:
  RecordData(const Schema& schema) {
    inline_data_ = new uint64_t[schema.num_inline_columns()];
    pointed_data_ = new uintptr_t[schema.num_pointer_columns()];

    VLOG(10) << "creating record with schema of " << schema.num_inline_columns()
             << ", " << schema.num_pointer_columns() << " columns";
    VLOG(10) << "inlined data @ " << inline_data_ << "; pointed data @ "
             << pointed_data_;
  };

  bool cmp(const RecordData& other, const Schema& schema) const {
    for (auto i = 0; i < schema.num_columns(); ++i) {
      std::pair<bool, size_t> inline_and_index = schema.RawColumnIndex(i);
      if (inline_and_index.first) {
        // inline value
        if (inline_data_[inline_and_index.second] !=
            other.inline_data_[inline_and_index.second]) {
          return false;
        }
      } else {
        // pointer-indirect value
        void* data_ptr =
            reinterpret_cast<void*>(pointed_data_[inline_and_index.second]);
        void* other_data_ptr = reinterpret_cast<void*>(
            other.pointed_data_[inline_and_index.second]);
        size_t size = Schema::size_of(schema.TypeOf(i), data_ptr);
        // XXX(malte): I don't think this is quite correct. std::string, for
        // example, stores a size as well as the data. A direct memcmp may not
        // compare the data, but bytewise equality of metadata + data.
        if (memcmp(data_ptr, other_data_ptr, size) != 0) {
          return false;
        }
      }
    }
    return true;
  }

  void Clear(const Schema& schema) {
    for (auto i = 0; i < schema.num_columns(); ++i) {
      if (!Schema::is_inlineable(schema.TypeOf(i))) {
        DeleteOwnedData(pointed_data_[schema.RawColumnIndex(i).second],
                        schema.TypeOf(i));
      }
    }
    delete pointed_data_;
    delete inline_data_;
  }

 private:
  uint64_t* inline_data_;
  uintptr_t* pointed_data_;

  void DeleteOwnedData(uintptr_t ptr, DataType type);

  friend class Record;
  FRIEND_TEST(RecordTest, DataRep);
};

class Record {
 public:
  Record(const Schema& schema, bool positive = true)
      : data_(RecordData(schema)),
        schema_(&schema),
        timestamp_(0),
        positive_(positive) {}

  ~Record() { data_.Clear(*schema_); }

  void* operator[](size_t index);
  const void* operator[](size_t index) const;
  void* at_mut(size_t index);
  const void* at(size_t index) const;

  bool positive() const { return positive_; }
  void set_positive(bool pos) { positive_ = pos; }
  int timestamp() const { return timestamp_; }
  void set_timestamp(int time) { timestamp_ = time; }
  const Schema& schema() const { return *schema_; }

  bool operator==(const Record& other) const {
    // records with different signs do not compare equal
    if (positive_ != other.positive_ || schema_ != other.schema_) {
      return false;
    }
    // relies on deep comparison between RecordData instances
    return data_.cmp(other.data_, *schema_);
  }
  bool operator!=(const Record& other) const { return !(*this == other); }

 private:
  // Data is interpreted according to schema stored outside the record.
  // RecordData itself only contains two pointers, one to the inline data
  // storage and one to the storage of pointers for referenced data.
  // [16 B]
  RecordData data_;
  // Schema for the record.
  // TODO(malte): figure out if we want to store this here.
  // [8 B]
  const Schema* schema_;
  // Timestamp of this record.
  // [4 B]
  int timestamp_;
  // Is this a positive or negative record?
  // [1 B]
  bool positive_;

  // *******
  // 3B SPARE on x86-64 due to 8B alignment
  // *******

  FRIEND_TEST(RecordTest, DataRep);
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_RECORD_H_
