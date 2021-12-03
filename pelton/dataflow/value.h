#ifndef PELTON_DATAFLOW_VALUE_H_
#define PELTON_DATAFLOW_VALUE_H_

#include <cstring>
#include <string>
#include <utility>

#include "glog/logging.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

class NullValue {
  // nothing to see here
};

class Value {
 public:
  // Actual value constructors.
  explicit Value(uint64_t v)
      : type_(sqlast::ColumnDefinition::Type::UINT), uint_(v) {}

  explicit Value(int64_t v)
      : type_(sqlast::ColumnDefinition::Type::INT), sint_(v) {}

  explicit Value(const std::string &v)
      : type_(sqlast::ColumnDefinition::Type::TEXT), str_(v) {}

  explicit Value(std::string &&v)
      : type_(sqlast::ColumnDefinition::Type::TEXT), str_(std::move(v)) {}

  explicit Value(sqlast::ColumnDefinition::Type type)
      : type_(type), is_null_(true) {}

  // Copies the underlying string.
  Value(const Value &o);
  Value &operator=(const Value &o);

  // Move moves the string.
  Value(Value &&o);
  Value &operator=(Value &&o);

  // Manually destruct string if value is a string.
  ~Value() {
    if (this->type_ == sqlast::ColumnDefinition::Type::TEXT) {
      this->str_.~basic_string();
    }
  }

  // Comparisons.
  bool operator==(const Value &other) const;
  bool operator!=(const Value &other) const { return !(*this == other); }
  bool operator<(const Value &other) const;

  // Hash to use as a key in absl hash tables.
  template <typename H>
  friend H AbslHashValue(H h, const Value &k) {
    if (k.is_null_) {
      return H::combine(std::move(h), "[nullptr]");
    }
    switch (k.type_) {
      case sqlast::ColumnDefinition::Type::UINT:
        return H::combine(std::move(h), k.uint_);
      case sqlast::ColumnDefinition::Type::INT:
        return H::combine(std::move(h), k.sint_);
      case sqlast::ColumnDefinition::Type::TEXT:
      case sqlast::ColumnDefinition::Type::DATETIME:
        return H::combine_contiguous(std::move(h), k.str_.c_str(),
                                     k.str_.size());
      default:
        LOG(FATAL) << "unimplemented pointed data hashing for Value";
    }
    return h;
  }

  // Data access.
  uint64_t GetUInt() const;
  int64_t GetInt() const;
  const std::string &GetString() const;
  bool IsNull() const { return this->is_null_; }

  std::string GetSqlString() const {
    if (this->is_null_)
      return "NULL";
    switch (this->type_) {
      case sqlast::ColumnDefinition::Type::UINT:
        return std::to_string(this->uint_);
      case sqlast::ColumnDefinition::Type::INT:
        return std::to_string(this->sint_);
      case sqlast::ColumnDefinition::Type::TEXT:
      case sqlast::ColumnDefinition::Type::DATETIME:
        // TODO(Justus): Make this actually escape the string!!!
        return "'" + this->str_ + "'";
      default:
        LOG(FATAL) << "unimplemented sql escape for Value";
    }
  }

  // Access the underlying type.
  sqlast::ColumnDefinition::Type type() const { return this->type_; }

  // For logging and printing...
  friend std::ostream &operator<<(std::ostream &os, const Value &k);

  // The size in memory.
  size_t SizeInMemory() const;

 private:
  sqlast::ColumnDefinition::Type type_;

  union {
    uint64_t uint_;
    int64_t sint_;
    std::string str_;
  };
  bool is_null_ = false;

  inline void CheckType(sqlast::ColumnDefinition::Type t) const {
    if (this->type_ != t) {
      LOG(FATAL) << "Type mismatch: value type is " << this->type_
                 << ", tried to access as " << t;
    }
  }
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_VALUE_H_
