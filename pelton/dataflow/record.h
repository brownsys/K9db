#ifndef PELTON_DATAFLOW_RECORD_H_
#define PELTON_DATAFLOW_RECORD_H_

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

class Record {
 public:
  // Variant with the same type options as the record data type, can be used
  // by external code to ensure same types are supported.
  using DataVariant = std::variant<std::string, uint64_t, int64_t>;
  static sqlast::ColumnDefinition::Type TypeOfVariant(const DataVariant &v);

  // No default constructor.
  Record() = delete;

  // No copy constructor. We want (expensive) record copying
  // to be explicit.
  Record(const Record &) = delete;
  Record &operator=(const Record &) = delete;

  // Records can move (e.g. for inserting into vectors and maps).
  Record(Record &&o)
      : data_(o.data_),
        schema_(o.schema_),
        timestamp_(o.timestamp_),
        positive_(o.positive_) {
    o.data_ = nullptr;
  }
  // Need to be able to move-assign for cases when std::vector<Record>
  // has elements deleted in the middle, and has to move the remaining
  // elements backwards to overwrite deleted element.
  Record &operator=(Record &&o) {
    CHECK(this->schema_ == o.schema_) << "Bad move assign record schema";
    // Free this.
    this->FreeRecordData();
    // Move things.
    this->data_ = o.data_;
    this->timestamp_ = o.timestamp_;
    this->positive_ = o.positive_;
    // Invalidate other.
    o.data_ = nullptr;
    return *this;
  }

  // Allocate memory but do not put any values in yet!
  explicit Record(const SchemaRef &schema, bool positive = true)
      : schema_(schema), timestamp_(0), positive_(positive) {
    this->data_ = new RecordData[schema.size()];
  }

  // Destructor: we have to manually destruct all unique_ptrs in unions
  // and delete the data array on the heap (unions do not call destructors
  // of their data automatically).
  ~Record() { this->FreeRecordData(); }

  // Explicit deep copying.
  Record Copy() const;

  // Data access.
  void SetUInt(uint64_t uint, size_t i);
  void SetInt(int64_t sint, size_t i);
  void SetString(std::unique_ptr<std::string> &&v, size_t i);
  uint64_t GetUInt(size_t i) const;
  int64_t GetInt(size_t i) const;
  const std::string &GetString(size_t i) const;

  // Data access with generic type.
  Key GetValue(size_t i) const;
  Key GetKey() const;

  // Data type transformation.
  void SetValue(const std::string &value, size_t i);

  // Accessors.
  const SchemaRef &schema() const { return this->schema_; }
  bool IsPositive() const { return this->positive_; }
  int GetTimestamp() const { return this->timestamp_; }

  // Equality: schema must be identical (pointer wise) and all values must be
  // equal.
  bool operator==(const Record &other) const;
  bool operator!=(const Record &other) const { return !(*this == other); }

  // For logging and printing...
  friend std::ostream &operator<<(std::ostream &os, const Record &r);

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

  // 29 bytes but with alignment it is really 32 bytes.
  RecordData *data_;  // [8 B]
  SchemaRef schema_;  // [16 B]
  int timestamp_;     // [4 B]
  bool positive_;     // [1 B]

  inline void CheckType(size_t i, sqlast::ColumnDefinition::Type t) const {
    CHECK_NE(this->data_, nullptr) << "Attempting to use moved record";
    if (this->schema_.TypeOf(i) != t) {
      LOG(FATAL) << "Type mismatch: record type is " << this->schema_.TypeOf(i)
                 << ", tried to access as " << t;
    }
  }

  inline void FreeRecordData() {
    if (this->data_ != nullptr) {
      // Destruct unique_ptrs.
      for (size_t i = 0; i < this->schema_.size(); i++) {
        const auto &type = this->schema_.column_types().at(i);
        if (type == sqlast::ColumnDefinition::Type::TEXT) {
          this->data_[i].str = std::unique_ptr<std::string>();
        }
      }
      // Delete allocated unions.
      delete[] this->data_;
      this->data_ = nullptr;
    }
  }
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_RECORD_H_
