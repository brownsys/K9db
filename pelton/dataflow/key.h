#ifndef PELTON_DATAFLOW_KEY_H_
#define PELTON_DATAFLOW_KEY_H_

#include <cstring>
#include <string>
#include <utility>

#include "pelton/dataflow/schema.h"

namespace pelton {
namespace dataflow {

class Key {
 public:
  explicit Key(uint64_t v) : inline_data_(v), type_(DataType::kUInt) {}
  explicit Key(int64_t v)
      : inline_data_(static_cast<int64_t>(v)), type_(DataType::kInt) {}
  explicit Key(const std::string& v)
      : pointed_data_(reinterpret_cast<const uint8_t*>(&v)),
        type_(DataType::kText) {}
  Key(uint64_t inline_data, DataType type)
      : inline_data_(inline_data), type_(type) {}
  Key(const void* pointed_data, DataType type)
      : pointed_data_(reinterpret_cast<const uint8_t*>(pointed_data)),
        type_(type) {}

  bool operator==(const Key& other) const {
    CHECK_EQ(type_, other.type_) << "comparing keys of different types!";
    if (Schema::is_inlineable(type_)) {
      return inline_data_ == other.inline_data_;
    } else {
      CHECK_NOTNULL(pointed_data_);
      switch (type_) {
        case DataType::kText: {
          auto this_str = reinterpret_cast<const std::string*>(pointed_data_);
          auto other_str =
              reinterpret_cast<const std::string*>(other.pointed_data_);
          return *this_str == *other_str;
        }
        default:
          LOG(FATAL) << "unimplemented pointed data comparison on Key";
      }
    }

    return false;
  }
  bool operator!=(const Key& other) const { return !(*this == other); }

  template <typename H>
  friend H AbslHashValue(H h, const Key& k) {
    if (Schema::is_inlineable(k.type_)) {
      return H::combine(std::move(h), k.inline_data_);
    } else {
      switch (k.type_) {
        case DataType::kText: {
          auto str = reinterpret_cast<const std::string*>(k.pointed_data_);
          return H::combine(std::move(h), str->c_str(), str->size());
        }
        default:
          LOG(FATAL) << "unimplemented pointed data hashing for Key";
      }
    }
    return h;
  }

  uint64_t as_uint() const {
    CheckType(DataType::kUInt);
    return inline_data_;
  }
  int64_t as_int() const {
    CheckType(DataType::kInt);
    return static_cast<int64_t>(inline_data_);
  }
  const std::string& as_string() const {
    CheckType(DataType::kText);
    return *reinterpret_cast<const std::string*>(pointed_data_);
  }

 private:
  union {
    uint64_t inline_data_;
    const uint8_t* pointed_data_;
  };

  DataType type_;

  inline void CheckType(DataType t) const {
    if (type_ != t) {
      LOG(FATAL) << "Type mismatch: key type is is " << type_
                 << ", tried to access as " << t;
    }
  }
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_KEY_H_
