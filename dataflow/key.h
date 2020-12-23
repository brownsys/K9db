#ifndef PELTON_DATAFLOW_KEY_H_
#define PELTON_DATAFLOW_KEY_H_

#include <cstring>

#include "dataflow/schema.h"

namespace dataflow {

class Key {
 public:
  Key(uint64_t v) : Key(reinterpret_cast<void*>(v), DataType::kUInt) {}
  Key(int64_t v) : Key(reinterpret_cast<void*>(v), DataType::kInt) {}
  Key(const std::string* v) : Key(v, DataType::kText) {}
  Key(const void* field, DataType type)
      : data_(reinterpret_cast<uint64_t>(field)), type_(type) {}

  bool operator==(const Key& other) const {
    CHECK_EQ(type_, other.type_) << "comparing keys of different types!";
    if (Schema::is_inlineable(type_)) {
      return data_ == other.data_;
    } else {
      // TODO(malte): this just compares Schema::size_of bytes at the pointer
      // location. We may need type-specific comparison for more elaborate
      // datatypes.
      void* dataptr = reinterpret_cast<void*>(data_);
      void* other_dataptr = reinterpret_cast<void*>(other.data_);
      if (Schema::size_of(type_, dataptr) !=
          Schema::size_of(other.type_, other_dataptr)) {
        return false;
      }
      // XXX(malte): I don't think this is quite correct. std::string, for
      // example, stores a size as well as the data. A direct memcmp may not
      // compare the data, but bytewise equality of metadata + data.
      return memcmp(dataptr, other_dataptr, Schema::size_of(type_, dataptr));
    }
  }
  bool operator!=(const Key& other) const { return !(*this == other); }

  template <typename H>
  friend H AbslHashValue(H h, const Key& k) {
    if (Schema::is_inlineable(k.type_)) {
      return H::combine(std::move(h), k.data_);
    } else {
      return H::combine(
          std::move(h), k.data_,
          Schema::size_of(k.type_, reinterpret_cast<void*>(k.data_)));
    }
  }

  uint64_t as_uint() const {
    CheckType(DataType::kUInt);
    return data_;
  }
  uint64_t as_int() const {
    CheckType(DataType::kInt);
    return static_cast<int64_t>(data_);
  }
  const std::string& as_string() const {
    CheckType(DataType::kText);
    return *reinterpret_cast<const std::string*>(data_);
  }

 private:
  uint64_t data_;
  DataType type_;

  inline void CheckType(DataType t) const {
    if (type_ != t) {
      LOG(FATAL) << "Type mismatch: key type is is " << type_
                 << ", tried to access as " << t;
    }
  }
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_KEY_H_
