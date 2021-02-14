#ifndef PELTON_DATAFLOW_RECORD_H_
#define PELTON_DATAFLOW_RECORD_H_

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <vector>
#include <utility>

#include "glog/logging.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

class Record {
 public:
  // No default constructor.
  Record() = delete;

  // No copy constructor.
  Record(const Record &) = delete;
  Record &operator=(const Record &) = delete;

  // Allocate memory but do not put any values in yet!
  explicit Record(const Schema &schema, bool positive = true)
      : schema_(schema), positive_(positive) {
    this->data_ = new RecordData[schema.Size()];
  }
  
  // Destructor: we have to manually destruct all unique_ptrs in unions
  // and delete the data array on the heap (unions do not call destructors
  // of their data automatically).
  ~Record() {
    // Destruct unique_ptrs.
    for (size_t i = 0; i < this->schema_.Size(); i++) {
      const auto &type = this->schema_.column_types().at(i);
      if (type == sqlast::ColumnDefinition::Type::TEXT) {
        this->data_[i].str = std::unique_ptr<std::string>();
      }
    }
    // Delete allocated unions.
    delete[] this->data_;
  }

  // Access schema and data.
  const Schema &schema() const {
    return this->schema_;
  }

  void SetUInt(uint64_t uint, size_t i) {
    CheckType(i, sqlast::ColumnDefinition::Type::UINT);
    this->data_[i].uint = uint;
  }  
  void SetInt(int64_t sint, size_t i) {
    CheckType(i, sqlast::ColumnDefinition::Type::INT);
    this->data_[i].sint = sint;
  }
  void SetString(std::unique_ptr<std::string> &&v, size_t i) {
    CheckType(i, sqlast::ColumnDefinition::Type::TEXT);
    this->data_[i].str = std::move(v);
  }
  
  uint64_t GetUInt(size_t i) const {
    CheckType(i, sqlast::ColumnDefinition::Type::UINT);
    return this->data_[i].uint;
  }
  int64_t GetInt(size_t i) const {
    CheckType(i, sqlast::ColumnDefinition::Type::INT);
    return this->data_[i].sint;
  }
  const std::string &GetString(size_t i) const {
    CheckType(i, sqlast::ColumnDefinition::Type::TEXT);
    return *(this->data_[i].str);
  }
  std::unique_ptr<std::string> &&ReleaseString(size_t i) && {
    CheckType(i, sqlast::ColumnDefinition::Type::TEXT);
    return std::move(this->data_[i].str);
  }

  Key GetKey() const {
    const std::vector<size_t> &keys = this->schema_.keys();
    CHECK_EQ(keys.size(), 1U);
    size_t key_index = keys.at(0);
    switch (this->schema_.TypeOf(key_index)) {
      case sqlast::ColumnDefinition::Type::UINT:
        return Key(this->data_[key_index].uint);
      case sqlast::ColumnDefinition::Type::INT:
        return Key(this->data_[key_index].sint);
      case sqlast::ColumnDefinition::Type::TEXT:
        if (this->data_[key_index].str) {
          return Key(*(this->data_[key_index].str));
        }
        return Key("");
      default:
        LOG(FATAL) << "Unsupported data type in key extraction!";
    }
    return Key(0UL);
  }

  // Access metadata.
  bool IsPositive() const {
    return this->positive_;
  }
  int GetTimestamp() const {
    return this->timestamp_;
  }

  // Equality: schema must be identical (pointer wise) and all values must be
  // equal.
  bool operator==(const Record &other) const {
    // Compare schemas (as pointers).
    if (this->schema_ != other.schema_) {
      return false;
    }
    // Compare data (deep compare).
    for (size_t i = 0; i < this->schema_.Size(); i++) {
      const auto &type = this->schema_.column_types().at(i);
      switch (type) {
        case sqlast::ColumnDefinition::Type::UINT:
          if (this->data_[i].uint != other.data_[i].uint) {
            return false;
          }
          break;
        case sqlast::ColumnDefinition::Type::INT:
          if (this->data_[i].sint != other.data_[i].sint) {
            return false;
          }
          break;
        case sqlast::ColumnDefinition::Type::TEXT:
          // If the pointers are not literally identical pointers.
          if (this->data_[i].str != other.data_[i].str) {
            // Either is null but not both.
            if (!this->data_[i].str || !other.data_[i].str) {
              return false;
            }
            // Compare contents.
            if (*this->data_[i].str != *other.data_[i].str) {
              return false;
            }
          }
          break;
        default:
          LOG(FATAL) << "Unsupported data type in record ==";
          return false;
      }
    }
    return true;
  }
  bool operator!=(const Record &other) const {
    return !(*this == other);
  }

  // For logging and printing...
  friend std::ostream& operator<<(std::ostream& os, const Record& r);

 private:
  union RecordData {
    uint64_t uint;
    int64_t sint;
    std::unique_ptr<std::string> str;
    // Initially garbage.
    RecordData() : str() {}
    // We cant tell if we are using str or not, destructing it must be done
    // by the Record class externally.
    ~RecordData() {}
  };

  // 21 bytes but with alignment it is really 24 bytes.
  RecordData *data_;      // [8 B]
  const Schema &schema_;  // [8 B]
  int timestamp_;         // [4 B]
  bool positive_;         // [1 B]
  
  inline void CheckType(size_t i, sqlast::ColumnDefinition::Type t) const {
    if (this->schema_.TypeOf(i) != t) {
      LOG(FATAL) << "Type mismatch: record type is " << this->schema_.TypeOf(i)
                 << ", tried to access as " << t;
    }
  }
};

// Printing a record to an output stream (e.g. std::cout).
inline std::ostream& operator<<(std::ostream &os,
                                const pelton::dataflow::Record &r) {
  os << "|";
  for (unsigned i = 0; i < r.schema_.Size(); ++i) {
    switch (r.schema_.TypeOf(i)) {
      case sqlast::ColumnDefinition::Type::UINT:
        os << r.data_[i].uint << "|";
        break;
      case sqlast::ColumnDefinition::Type::INT:
        os << r.data_[i].sint << "|";
        break;
      case sqlast::ColumnDefinition::Type::TEXT:
        os << *r.data_[i].str << "|";
        break;
      default:
        LOG(FATAL) << "Unsupported data type in record << operator";
    }
  }
  return os;
}

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_RECORD_H_
