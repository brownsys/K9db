#ifndef PELTON_DATAFLOW_RECORD_H_
#define PELTON_DATAFLOW_RECORD_H_

#include <cstdint>

#include <cassert>
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
    LOG(INFO) << "creating record with schema of "
              << schema.num_inline_columns() << ", "
              << schema.num_pointer_columns() << " columns";
    inline_data_ = new uint64_t[schema.num_inline_columns()];
    pointed_data_ = new uintptr_t[schema.num_pointer_columns()];
  };

  void Clear(const Schema& schema) {
    for (auto i = 0; i < schema.num_pointer_columns(); ++i) {
      // delete pointed_data_[i];
    }
    delete pointed_data_;
    delete inline_data_;
  }

 private:
  uint64_t* inline_data_;
  uintptr_t* pointed_data_;

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
  void set_positive(bool pos) { positive_ = pos; };
  int timestamp() const { return timestamp_; }
  void set_timestamp(int time) { timestamp_ = time; };

  bool operator==(const Record& other) const {
    // XXX(malte): deep compare of records
    return false;
  }

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
