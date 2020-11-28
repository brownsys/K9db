#ifndef PELTON_DATAFLOW_KEY_H_
#define PELTON_DATAFLOW_KEY_H_

#include <cstring>

#include "dataflow/schema.h"

namespace dataflow {

class Key {
 public:
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

 private:
  uint64_t data_;
  DataType type_;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_KEY_H_
