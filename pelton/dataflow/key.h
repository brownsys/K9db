#ifndef PELTON_DATAFLOW_KEY_H_
#define PELTON_DATAFLOW_KEY_H_

#include <cstring>
#include <string>
#include <utility>

#include "glog/logging.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

class Key {
 public:

  explicit Key(uint64_t v)
      : type_(sqlast::ColumnDefinition::Type::UINT), uint_(v) {}

  explicit Key(int64_t v)
      : type_(sqlast::ColumnDefinition::Type::INT), sint_(v) {}

explicit Key(uint8_t v) : Key(static_cast<uint64_t>(v)) {} 
explicit Key(uint16_t v) : Key(static_cast<uint64_t>(v)) {}
  explicit Key(uint32_t v) : Key(static_cast<uint64_t>(v)) {}

explicit Key(int8_t v) : Key(static_cast<int64_t>(v))  {}
explicit Key(int16_t v) : Key(static_cast<int64_t>(v))  {}
  explicit Key(int32_t v) : Key(static_cast<int64_t>(v))  {}
  

  explicit Key(const std::string &v)
      : type_(sqlast::ColumnDefinition::Type::TEXT), str_(v) {}

  explicit Key(std::string &&v)
      : type_(sqlast::ColumnDefinition::Type::TEXT), str_(v) {}

  // Must be copyable to be used as a key for absl maps.
  Key(const Key &o);
  Key &operator=(const Key &o);

  // Move moves the string.
  Key(Key &&o);
  Key &operator=(Key &&o);

  // Manually destruct string if key is a string.
  ~Key() {
    if (this->type_ == sqlast::ColumnDefinition::Type::TEXT) {
      this->str_.~basic_string();
    }
  }

  // Comparisons.
  bool operator==(const Key &other) const;
  bool operator!=(const Key &other) const { return !(*this == other); }

  // Hash to use as key in absl hash tables.
  template <typename H>
  friend H AbslHashValue(H h, const Key &k) {
    switch (k.type_) {
      case sqlast::ColumnDefinition::Type::UINT:
        return H::combine(std::move(h), k.uint_);
      case sqlast::ColumnDefinition::Type::INT:
        return H::combine(std::move(h), k.sint_);
      case sqlast::ColumnDefinition::Type::TEXT:
        return H::combine(std::move(h), k.str_.c_str(), k.str_.size());
      default:
        LOG(FATAL) << "unimplemented pointed data hashing for Key";
    }
    return h;
  }

  // Data access.
  uint64_t GetUInt() const;
  int64_t GetInt() const;
  const std::string &GetString() const;

  // Access the underlying type.
  sqlast::ColumnDefinition::Type type() const { return this->type_; }

 private:
  sqlast::ColumnDefinition::Type type_;

  union {
    uint64_t uint_;
    int64_t sint_;
    std::string str_;
  };

  inline void CheckType(sqlast::ColumnDefinition::Type t) const {
    if (this->type_ != t) {
      LOG(FATAL) << "Type mismatch: key type is " << this->type_
                 << ", tried to access as " << t;
    }
  }
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_KEY_H_
