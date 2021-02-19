#ifndef PELTON_DATAFLOW_SCHEMA_H_
#define PELTON_DATAFLOW_SCHEMA_H_

#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

class SchemaRef;

class SchemaOwner {
 public:
  // Construct from a CREATE TABLE statement.
  explicit SchemaOwner(const sqlast::CreateTable &table);

  // Construct from schema data directly.
  SchemaOwner(const std::vector<std::string> &column_names,
              const std::vector<sqlast::ColumnDefinition::Type> &column_types,
              const std::vector<ColumnID> &keys) {
    this->ptr_ = new SchemaData(column_names, column_types, keys);
  }

  // An owner is only movable, which moves ownership of the underlying pointer.
  SchemaOwner(const SchemaOwner &) = delete;
  SchemaOwner &operator=(const SchemaOwner &) = delete;
  SchemaOwner(SchemaOwner &&other) { *this = std::move(other); }
  SchemaOwner &operator=(SchemaOwner &&other) {
    this->ptr_ = other.ptr_;
    other.ptr_ = nullptr;
    return *this;
  }

  // Destructor.
  virtual ~SchemaOwner() {
    if (this->ptr_ != nullptr) {
      delete this->ptr_;
    }
  }

  // Equality is underlying pointer equality.
  bool operator==(const SchemaOwner &other) const {
    return this->ptr_ == other.ptr_;
  }
  bool operator!=(const SchemaOwner &other) const {
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

 protected:
  SchemaOwner() : ptr_(nullptr) {}

  // Schema data is a protected struct inside SchemaOwner, it is never exposed
  // directly.
  struct SchemaData {
    std::vector<std::string> column_names;
    std::vector<sqlast::ColumnDefinition::Type> column_types;
    std::vector<ColumnID> keys;
    // Constructors.
    SchemaData() {}
    SchemaData(const std::vector<std::string> &cn,
               const std::vector<sqlast::ColumnDefinition::Type> &ct,
               const std::vector<ColumnID> &ks)
        : column_names(cn), column_types(ct), keys(ks) {
      if (column_names.size() != column_types.size()) {
        LOG(FATAL) << "Incosistent number of columns in schema!";
      }
    }
    // Not copyable or movable, we will only pass this around by reference or
    // pointer.
    SchemaData(const SchemaData &) = delete;
    SchemaData &operator=(const SchemaData &) = delete;
    SchemaData(SchemaData &&) = delete;
    SchemaData &operator=(SchemaData &&) = delete;
  };

  // Schema wrapper classes store a ptr to the underlying SchemaData.
  // SchemaOwner manages creation and deletion of this pointer, while
  // SchemaRef only uses it without management.
  SchemaData *ptr_;

  friend SchemaRef;
};

class SchemaRef : public SchemaOwner {
 public:
  // Can only be constructor from an owner!
  explicit SchemaRef(const SchemaOwner &other) : SchemaOwner() {
    this->ptr_ = other.ptr_;
  }

  // Can be moved and copied!
  SchemaRef(const SchemaRef &other) { *this = other; }
  SchemaRef &operator=(const SchemaRef &other) {
    this->ptr_ = other.ptr_;
    return *this;
  }

  SchemaRef(SchemaRef &&other) { *this = std::move(other); }
  SchemaRef &operator=(SchemaRef &&other) {
    this->ptr_ = other.ptr_;
    other.ptr_ = nullptr;
    return *this;
  }

  // Unmanaged ptr_.
  ~SchemaRef() {
    this->ptr_ = nullptr;  // Stops base destructor from deleting ptr_.
  }
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_SCHEMA_H_
