#ifndef K9DB_DATAFLOW_RECORD_H_
#define K9DB_DATAFLOW_RECORD_H_

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "glog/logging.h"
#include "k9db/dataflow/key.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/types.h"
#include "k9db/sqlast/ast.h"
#include "k9db/util/type_utils.h"

namespace k9db {
namespace dataflow {

// Pass instances of this to Record(<schema>, <positive>, <data...>) to indicate
// null values.
class NullValue {
  // nothing to see here
};

class Record {
 public:
  // No default constructor.
  Record() = delete;

  // No copy constructor. We want (expensive) record copying
  // to be explicit.
  Record(const Record &) = delete;
  Record &operator=(const Record &) = delete;

  // Records can move (e.g. for inserting into vectors and maps).
  Record(Record &&o)
      : data_(o.data_),
        bitmap_(o.bitmap_),
        schema_(o.schema_),
        timestamp_(o.timestamp_),
        positive_(o.positive_) {
    o.data_ = nullptr;
    o.bitmap_ = nullptr;
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
    this->bitmap_ = o.bitmap_;
    // Invalidate other.
    o.data_ = nullptr;
    o.bitmap_ = nullptr;
    return *this;
  }

  // Allocate memory but do not put any values in yet!
  explicit Record(const SchemaRef &schema, bool positive = true)
      : schema_(schema), timestamp_(0), positive_(positive) {
    this->data_ = new RecordData[schema.size()];
    this->bitmap_ = nullptr;
  }

  // Create record and set all the data together.
  template <typename... Args>
  Record(const SchemaRef &schema, bool positive, Args &&... ts)
      : Record(schema, positive) {
    this->SetData(std::forward<Args>(ts)...);
  }

  // Destructor: we have to manually destruct all unique_ptrs in unions
  // and delete the data array on the heap (unions do not call destructors
  // of their data automatically).
  ~Record() { this->FreeRecordData(); }

  // Helper function to create a record consisting onyl of nulls
  static Record NULLRecord(const SchemaRef &schema, bool positive = true) {
    Record r(schema, positive);
    for (size_t i = 0; i < schema.size(); ++i) {
      r.SetNull(true, i);
    }
    return r;
  }

  // Explicit deep copying.
  Record Copy() const;

  // Set all data in one shot regardless of types and counts.
  template <typename... Args>
  void SetData(Args &&... ts) {
    CHECK_NOTNULL(this->data_);
    if constexpr (sizeof...(ts) > 0) {
      SetDataRecursive(0, std::forward<Args>(ts)...);
    }
  }

  // Data access.
  void SetUInt(uint64_t uint, size_t i);
  void SetInt(int64_t sint, size_t i);
  void SetString(std::unique_ptr<std::string> &&v, size_t i);
  void SetDateTime(std::unique_ptr<std::string> &&v, size_t i);
  uint64_t GetUInt(size_t i) const;
  int64_t GetInt(size_t i) const;
  const std::string &GetString(size_t i) const;
  const std::string &GetDateTime(size_t i) const;

  inline bool IsNull(size_t i) const {
    if (!bitmap_) return false;

    CHECK(bitmap_);
    auto bitmap_block_idx = i / 64;
    auto bitmap_idx = i % 64;
    CHECK_LT(bitmap_block_idx, NumBits());
    return bitmap_[bitmap_block_idx] & (0x1ul << bitmap_idx);
  }

  inline void SetNull(bool isnull, size_t i) {
    // shortcut: if isnull=false and no bitmap, no change
    if (!isnull && !bitmap_) return;

    // lazy init bitmap
    lazyBitmap();

    CHECK(bitmap_);
    auto bitmap_block_idx = i / 64;
    auto bitmap_idx = i % 64;
    CHECK_LT(bitmap_block_idx, NumBits());
    if (isnull)
      bitmap_[bitmap_block_idx] |= (0x1ul << bitmap_idx);
    else
      bitmap_[bitmap_block_idx] &= ~(0x1ul << bitmap_idx);

    // compact bitmap, i.e. when all fields become 0, then release bitmap
    // --> important when using == comparison invariant to remove a couple
    // checks there as well.
    uint64_t reduced_bitmap = 0;
    for (unsigned i = 0; i < NumBits(); ++i) reduced_bitmap |= bitmap_[i];
    if (0 == reduced_bitmap) {
      delete[] bitmap_;
      bitmap_ = nullptr;
    }
  }

  // The size of the record in bytes.
  size_t SizeInMemory() const;

  // Data access with generic type.
  Key GetKey() const;
  Key GetValues(const std::vector<ColumnID> &cols) const;
  sqlast::Value GetValue(ColumnID col) const;

  // Data type transformation.
  void SetValue(const sqlast::Value &value, size_t i);

  // Deterministic hashing for partitioning / mutli-threading.
  size_t Hash(const std::vector<ColumnID> &cols) const;

  // Accessors.
  void SetPositive(bool positive) { this->positive_ = positive; }
  const SchemaRef &schema() const { return this->schema_; }
  bool IsPositive() const { return this->positive_; }
  int GetTimestamp() const { return this->timestamp_; }

  // Create a new record resulting from applying the update to this record.
  // Updating the sequence given an update statement and schema.
  using UpdateMap = std::unordered_map<std::string, const sqlast::Expression *>;
  Record Update(const UpdateMap &updates) const;

  // Equality: schema must be identical (pointer wise) and all values must be
  // equal.
  bool operator==(const Record &other) const;
  bool operator!=(const Record &other) const { return !(*this == other); }

  // For logging and printing...
  friend std::ostream &operator<<(std::ostream &os, const Record &r);

  // Custom comparison between records (for ordering).
  class Compare {
   public:
    explicit Compare(const std::vector<ColumnID> &cols) : cols_(cols) {}
    bool operator()(const Record &l, const Record &r) const {
      CHECK_EQ(l.schema(), r.schema());
      const auto &types = l.schema().column_types();
      for (size_t i : this->cols_) {
        bool l_null = l.IsNull(i);
        bool r_null = r.IsNull(i);
        if (l_null && !r_null) {
          return true;
        } else if (!l_null && r_null) {
          return false;
        } else if (l_null && r_null) {
          continue;
        }
        switch (types.at(i)) {
          case sqlast::ColumnDefinition::Type::UINT:
            if (l.data_[i].uint < r.data_[i].uint) {
              return true;
            } else if (l.data_[i].uint != r.data_[i].uint) {
              return false;
            }
            break;
          case sqlast::ColumnDefinition::Type::INT:
            if (l.data_[i].sint < r.data_[i].sint) {
              return true;
            } else if (l.data_[i].sint != r.data_[i].sint) {
              return false;
            }
            break;
          case sqlast::ColumnDefinition::Type::TEXT:
          case sqlast::ColumnDefinition::Type::DATETIME: {
            if (l.data_[i].str->size() != r.data_[i].str->size()) {
              return l.data_[i].str->size() < r.data_[i].str->size();
            }
            for (size_t j = 0; j < l.data_[i].str->size(); j++) {
              if (l.data_[i].str->at(j) < r.data_[i].str->at(j)) {
                return true;
              } else if (l.data_[i].str->at(j) != r.data_[i].str->at(j)) {
                return false;
              }
            }
            break;
          }
        }
      }
      return false;
    }
    const std::vector<ColumnID> &cols() const { return this->cols_; }

   private:
    std::vector<ColumnID> cols_;
  };

 private:
  // Recursive helper used in SetData(...).
  template <typename Arg, typename... Args>
  void SetDataRecursive(size_t index, Arg &&t, Args &&... ts) {
    if (index >= this->schema_.size()) {
      LOG(FATAL) << "Record data received too many arguments";
    }
    if constexpr (std::is_same<std::remove_reference_t<Arg>,
                               NullValue>::value) {
      this->SetNull(true, index);
    } else {
      // Make sure Arg is of the correct type.
      switch (this->schema_.TypeOf(index)) {
        case sqlast::ColumnDefinition::Type::UINT:
          if constexpr (std::is_same<std::remove_reference_t<Arg>,
                                     uint64_t>::value) {
            this->data_[index].uint = t;
          } else {
            LOG(FATAL) << "Type mismatch in SetData at index " << index
                       << ", expected " << this->schema_.TypeOf(index)
                       << ", got " << util::TypeNameFor(t);
          }
          break;
        case sqlast::ColumnDefinition::Type::INT:
          if constexpr (std::is_same<std::remove_reference_t<Arg>,
                                     int64_t>::value) {
            this->data_[index].sint = t;
          } else {
            LOG(FATAL) << "Type mismatch in SetData at index " << index
                       << ", expected " << this->schema_.TypeOf(index)
                       << ", got " << util::TypeNameFor(t);
          }
          break;
        case sqlast::ColumnDefinition::Type::TEXT:
          if constexpr (std::is_same<std::remove_reference_t<Arg>,
                                     std::unique_ptr<std::string>>::value) {
            this->data_[index].str = std::move(t);
          } else {
            LOG(FATAL) << "Type mismatch in SetData at index " << index
                       << ", expected " << this->schema_.TypeOf(index)
                       << ", got " << util::TypeNameFor(t);
          }
          break;
        case sqlast::ColumnDefinition::Type::DATETIME:
          if constexpr (std::is_same<std::remove_reference_t<Arg>,
                                     std::unique_ptr<std::string>>::value) {
            this->data_[index].str = std::move(t);
          } else {
            LOG(FATAL) << "Type mismatch in SetData at index " << index
                       << ", expected " << this->schema_.TypeOf(index)
                       << ", got " << util::TypeNameFor(t);
          }
          break;
        default:
          LOG(FATAL) << "Unsupported data type in SetData";
      }
    }
    // Handle the remaining ts.
    if constexpr (sizeof...(ts) > 0) {
      SetDataRecursive(index + 1, std::forward<Args>(ts)...);
    } else {
      if (this->schema_.size() != index + 1) {
        LOG(FATAL) << "Record data received too few arguments";
      }
    }
  }

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

  // 37 bytes but with alignment it is really 40 bytes.
  RecordData *data_;  // [8 B]
  uint64_t *bitmap_;  // [8 B]
  SchemaRef schema_;  // [8 B]
  int timestamp_;     // [4 B]
  bool positive_;     // [1 B]

  inline size_t NumBits() const { return schema_.size() / 64 + 1; }

  inline void CheckType(size_t i, sqlast::ColumnDefinition::Type t) const {
    CHECK_NOTNULL(this->data_);
    if (this->schema_.TypeOf(i) != t) {
      LOG(FATAL) << "Type mismatch: record type is " << this->schema_.TypeOf(i)
                 << ", tried to access as " << t;
    }
  }

  inline void FreeRecordData() {
    // Free bitmap
    if (this->bitmap_ != nullptr) {
      delete[] this->bitmap_;
      this->bitmap_ = nullptr;
    }

    if (this->data_ != nullptr) {
      // Destruct unique_ptrs.
      for (size_t i = 0; i < this->schema_.size(); i++) {
        const auto &type = this->schema_.column_types().at(i);
        if (type == sqlast::ColumnDefinition::Type::TEXT ||
            type == sqlast::ColumnDefinition::Type::DATETIME) {
          this->data_[i].str = std::unique_ptr<std::string>();
        }
      }
      // Delete allocated unions.
      delete[] this->data_;
      this->data_ = nullptr;
    }
  }

  void lazyBitmap() {
    if (!this->bitmap_) {
      this->bitmap_ = new uint64_t[NumBits()];
      memset(bitmap_, 0, NumBits() * sizeof(uint64_t));
    }
  }
};

}  // namespace dataflow
}  // namespace k9db

#endif  // K9DB_DATAFLOW_RECORD_H_
