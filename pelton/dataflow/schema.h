#ifndef PELTON_DATAFLOW_SCHEMA_H_
#define PELTON_DATAFLOW_SCHEMA_H_

#include <list>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

// We never use this directly in host code.
struct SchemaData {
  std::vector<std::string> column_names;
  std::vector<sqlast::ColumnDefinition::Type> column_types;
  std::vector<ColumnID> keys;
  // Constructors.
  SchemaData(const std::vector<std::string> &cn,
             const std::vector<sqlast::ColumnDefinition::Type> &ct,
             const std::vector<ColumnID> &ks)
      : column_names(cn), column_types(ct), keys(ks) {
    if (column_names.size() != column_types.size()) {
      LOG(FATAL) << "Incosistent number of columns in schema!";
    }
  }
  // Semantic equality (not pointer based).
  bool Is(const std::vector<std::string> &cn,
          const std::vector<sqlast::ColumnDefinition::Type> &ct,
          const std::vector<ColumnID> &ks) const {
    return column_names == cn && column_types == ct && keys == ks;
  }
};

// Forward declartion so that factory can be a friend.
class SchemaFactory;

class SchemaRef {
 public:
  // Empty SchemaRef: this is used to create a temporary so that Operators can
  // compute their output schema after creation.
  SchemaRef() : ptr_(nullptr) {}

  // Can be moved and copied!
  SchemaRef(const SchemaRef &other) = default;
  SchemaRef &operator=(const SchemaRef &other) = default;
  SchemaRef(SchemaRef &&other) = default;
  SchemaRef &operator=(SchemaRef &&other) = default;

  // Equality is underlying pointer equality.
  operator bool() const { return this->ptr_ != nullptr; }
  bool operator==(const SchemaRef &other) const {
    return this->ptr_ == other.ptr_;
  }
  bool operator!=(const SchemaRef &other) const {
    return this->ptr_ != other.ptr_;
  }

  // Accessors.
  const std::vector<std::string> &column_names() const;
  const std::vector<sqlast::ColumnDefinition::Type> &column_types() const;
  const std::vector<ColumnID> &keys() const;
  size_t size() const;

  // Accessor by index.
  sqlast::ColumnDefinition::Type TypeOf(size_t i) const;
  const std::string &NameOf(size_t i) const;

  // Accessor by column name.
  size_t IndexOf(const std::string &column_name) const;

  // For logging and printing...
  friend std::ostream &operator<<(std::ostream &os, const SchemaRef &r);

 private:
  // Can only be constructor from an existing SchemaData!
  explicit SchemaRef(const SchemaData *ptr) : ptr_(ptr) {}

  // SchemaRef is a wrapper class that stores an unmanged ptr to the underlying
  // SchemaData. An instance of SchemaData may be shared between instances of
  // SchemaRef.
  const SchemaData *ptr_;

  // SchemaFactory can use private constructor!
  friend SchemaFactory;
};

// We create schemas via this factory.
class SchemaFactory {
 public:
  static SchemaRef Create(const sqlast::CreateTable &table);
  static SchemaRef Create(
      const std::vector<std::string> &column_names,
      const std::vector<sqlast::ColumnDefinition::Type> &column_types,
      const std::vector<ColumnID> &keys);

 private:
  static std::list<SchemaData> SCHEMAS;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_SCHEMA_H_
