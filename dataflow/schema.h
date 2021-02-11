#ifndef PELTON_DATAFLOW_SCHEMA_H_
#define PELTON_DATAFLOW_SCHEMA_H_

#include <string>
#include <vector>

#include "glog/logging.h"

#include "dataflow/types.h"
#include "shards/sqlast/ast.h"

#include "absl/container/flat_hash_map.h"

namespace dataflow {

enum DataType {
  kUInt,
  kInt,
  kText,
  kDatetime,
};

DataType StringToDataType(const std::string &type_name);

class Schema;
class SchemaFactory;

class SchemaFactory {
public:
    static const Schema& create_or_get(const std::vector<DataType>& column_types);
    static const Schema& undefined_schema() { return create_or_get({}); }

    static SchemaFactory& instance() {
      static SchemaFactory the_one_and_only;
      return the_one_and_only;
    }

    ~SchemaFactory();

private:
    absl::flat_hash_map<std::vector<DataType>, Schema*> schemas_;
    SchemaFactory() {}
};

class Schema {
 public:

  explicit Schema() : num_inline_columns_(0), num_pointer_columns_(0) {}
  explicit Schema(std::vector<DataType> columns);
  explicit Schema(std::vector<DataType> columns,
                  std::vector<std::string> names);
  explicit Schema(const shards::sqlast::CreateTable &table_description);

  friend class SchemaFactory;

  Schema(const Schema& other)
      : types_(other.types_),
        names_(other.names_),
        key_columns_(other.key_columns_),
        num_inline_columns_(other.num_inline_columns_),
        num_pointer_columns_(other.num_pointer_columns_),
        true_indices_(other.true_indices_) {}

  Schema(Schema&& other) = default;
  Schema& operator=(const Schema& other) {
    types_ = other.types_;
    names_ = other.names_;
    key_columns_ = other.key_columns_;
    num_inline_columns_ = other.num_inline_columns_;
    num_pointer_columns_ = other.num_pointer_columns_;
    true_indices_ = other.true_indices_;
    return *this;
  }

  static inline bool is_inlineable(DataType t) {
    switch (t) {
      case kUInt:
      case kInt:
        return true;
      default:
        return false;
    }
  }

  static inline size_t size_of(DataType t, const void* data) {
    switch (t) {
      case kText: {
        // TODO(malte): this returns the string's size, but discounts metadata
        // and small string optimizations of std::string; when we move to a
        // length+buffer representation, it will work better.
        const std::string* s = reinterpret_cast<const std::string*>(data);
        return s->size();
      }
      case kUInt:
        return sizeof(uint64_t);
      case kInt:
        return sizeof(int64_t);
      default:
        LOG(FATAL) << "unimplemented";
    }
    return 0;
  }

  bool operator==(const Schema& other) const { return types_ == other.types_; }
  bool operator!=(const Schema& other) const { return !(*this == other); }

  DataType TypeOf(size_t index) const {
    BoundsCheckIndex(index);
    return types_[index];
  }

  const std::string &NameOf(size_t index) const {
    BoundsCheckIndex(index);
    return names_[index];
  }

  std::pair<bool, size_t> RawColumnIndex(size_t index) const {
    BoundsCheckIndex(index);
    return true_indices_[index];
  }

  size_t num_columns() const {
    return num_inline_columns_ + num_pointer_columns_;
  }
  size_t num_inline_columns() const { return num_inline_columns_; }
  size_t num_pointer_columns() const { return num_pointer_columns_; }
  const std::vector<ColumnID> key_columns() const { return key_columns_; }
  void set_key_columns(std::vector<ColumnID> columns) {
    key_columns_ = columns;
  }

  bool is_undefined() const { return types_.empty() && num_inline_columns_ == 0 && num_pointer_columns_ == 0; }

 private:
  std::vector<DataType> types_;
  std::vector<std::string> names_;
  std::vector<ColumnID> key_columns_;
  size_t num_inline_columns_;
  size_t num_pointer_columns_;
  std::vector<std::pair<bool, size_t>> true_indices_;

  inline void BoundsCheckIndex(size_t index) const {
    CHECK_LT(index, true_indices_.size())
        << "column index " << index << " out of bounds";
  }
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_SCHEMA_H_
