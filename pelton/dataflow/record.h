#ifndef PELTON_DATAFLOW_RECORD_H_
#define PELTON_DATAFLOW_RECORD_H_

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest_prod.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class Record;
class RecordData;

class RecordData {
 public:
  RecordData() = delete;

  explicit RecordData(const Schema& schema) {
    inline_data_ = new uint64_t[schema.num_inline_columns()];
    pointed_data_ = new uintptr_t[schema.num_pointer_columns()];

    VLOG(10) << "creating record with schema of " << schema.num_inline_columns()
             << ", " << schema.num_pointer_columns() << " columns";
    VLOG(10) << "inlined data @ " << inline_data_ << "; pointed data @ "
             << pointed_data_;
  }

  // copy construction implemented by `Record`; should never copy `RecordData`
  // directly.
  RecordData(const RecordData&) = delete;

  bool compare_equal(const Schema& schema, const RecordData& other,
                     const Schema& otherSchema) const {
    if (schema.num_pointer_columns() != otherSchema.num_pointer_columns() ||
        schema.num_inline_columns() != otherSchema.num_inline_columns())
      return false;

    // simply compare inline elements via memcmp.
    if (0 != memcmp(inline_data_, other.inline_data_,
                    sizeof(uint64_t) * schema.num_inline_columns()))
      return false;

    // for pointed elements, need to perform deep compare.
    // => b.c. pointed elements are buggy, skip
    if (schema.num_pointer_columns() > 0) {
      LOG(FATAL) << " not yet implemeneted ";
    }

    return true;

    // this is wrong, deprecate.
    for (size_t i = 0; i < schema.num_columns(); ++i) {
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
        size_t other_size = Schema::size_of(schema.TypeOf(i), other_data_ptr);
        // XXX(malte): I don't think this is quite correct. std::string, for
        // example, stores a size as well as the data. A direct memcmp may not
        // compare the data, but bytewise equality of metadata + data.
        if (memcmp(data_ptr, other_data_ptr, std::min(size, other_size)) != 0) {
          return false;
        }
      }
    }
    return true;
  }

  void Clear(const Schema& schema) {
    // for some reason always pointed_data is allocated. Only delete if actual
    // entries are there! else this leads to a bug...
    if (schema.num_pointer_columns() > 0) {
      for (size_t i = 0; i < schema.num_columns(); ++i) {
        if (!Schema::is_inlineable(schema.TypeOf(i))) {
          DeleteOwnedData(pointed_data_[schema.RawColumnIndex(i).second],
                          schema.TypeOf(i));
        }
      }
    }

    delete[] pointed_data_;
    delete[] inline_data_;
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
  Record() = delete;

  explicit Record(const Schema& schema, bool positive = true)
      : data_(RecordData(schema)),
        schema_(&schema),
        timestamp_(0),
        positive_(positive) {}

  Record(const Record& other)
      : data_(RecordData(other.schema())),
        schema_(other.schema_),
        timestamp_(other.timestamp_),
        positive_(other.positive_) {
    deep_copy(other);
  }

  Record& operator=(const Record& other) {
    if (this == &other) return *this;

    delete[] data_.inline_data_;
    delete[] data_.pointed_data_;

    data_.inline_data_ = new uint64_t[other.schema().num_inline_columns()];
    data_.pointed_data_ = new uintptr_t[other.schema().num_pointer_columns()];

    deep_copy(other);

    return *this;
  }

  ~Record() { data_.Clear(*schema_); }

  // These invoke `at` and `at_mut`
  void* operator[](size_t index);
  const void* operator[](size_t index) const;

  // These are RAW interfaces, which return the *address* of the column data
  // in the record's internal buffer. Note that this is a pointer to data for
  // inline data types, but a pointer to a *pointer* for pointer-indirect values
  // like strings. The caller must therefore know the type of the data to
  // interpret the return value.
  //
  // Use of these raw functions is *discouraged*; proceed at your own peril.
  void* at_mut(size_t index);
  const void* at(size_t index) const;

  // Returns byte size of data in column `index`
  size_t size_at(size_t index) const;

  bool positive() const { return positive_; }
  void set_positive(bool pos) { positive_ = pos; }
  int timestamp() const { return timestamp_; }
  void set_timestamp(int time) { timestamp_ = time; }
  const Schema& schema() const { return *schema_; }

  const std::pair<Key, bool> GetKey() const;

  bool operator==(const Record& other) const {
    // schemas different?
    if (schema() != other.schema()) return false;

    // records with different signs do not compare equal
    if (positive_ != other.positive_) {
      return false;
    }
    // relies on deep comparison between RecordData instances
    return data_.compare_equal(*schema_, other.data_, *other.schema_);
  }
  bool operator!=(const Record& other) const { return !(*this == other); }

  friend std::ostream& operator<<(std::ostream& os, const Record& r);

  // Type-based accessor/mutator calls. If used on a column that has a different
  // type in the schema, these fail via LOG(FATAL).
  void set_uint(size_t index, uint64_t v);
  uint64_t as_uint(size_t index) const;
  void set_int(size_t index, int64_t v);
  int64_t as_int(size_t index) const;
  // TODO(malte): consider swapping std::string for a length + buffer
  void set_string(size_t index, std::string* s);
  std::string* as_string(size_t index) const;

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

  inline void CheckType(size_t index, DataType t) const {
    if (schema_->TypeOf(index) != t) {
      LOG(FATAL) << "Type mismatch: column is " << schema_->TypeOf(index)
                 << ", tried to access as " << t;
    }
  }

  inline void deep_copy(const Record& other) {
    schema_ = other.schema_;
    timestamp_ = other.timestamp_;
    positive_ = other.positive_;

    // copy inline members
    std::memcpy(data_.inline_data_, other.data_.inline_data_,
                sizeof(uint64_t) * schema_->num_inline_columns());

    // this is wrong. Destructor deletes deleted data. hence, need to perform
    // proper deepcopy... std::memcpy(data_.pointed_data_,
    // other.data_.pointed_data_, sizeof(uint64_t) *
    // schema_->num_pointer_columns());
    for (unsigned i = 0; i < other.schema().num_pointer_columns(); ++i) {
      // deep copy string (only allowed pointer type)
      throw std::runtime_error(
          "not yet supported, proper string mgmt needs to be implemented "
          "first...!");
    }
  }

  FRIEND_TEST(RecordTest, DataRep);
};

inline std::ostream& operator<<(std::ostream& os, const dataflow::Record& r) {
  os << "|";
  for (unsigned i = 0; i < r.schema().num_columns(); ++i) {
    switch (r.schema().TypeOf(i)) {
      case DataType::kInt: {
        os << r.as_int(i) << "|";
        break;
      }
      case DataType::kUInt: {
        os << r.as_uint(i) << "|";
        break;
      }
      case DataType::kText: {
        auto str = r.as_string(i) ? *r.as_string(i) : "NULL";
        os << str << "|";
        break;
      }
      default:
        LOG(FATAL) << "unsupported data type in ostream << operator";
    }
  }
  return os;
}

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_RECORD_H_
