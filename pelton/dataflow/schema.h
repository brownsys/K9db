#ifndef PELTON_DATAFLOW_SCHEMA_H_
#define PELTON_DATAFLOW_SCHEMA_H_

#include <cassert>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

class Schema {
 public:
  explicit Schema(const sqlast::CreateTable &table) {
    const auto &cols = table.GetColumns();
    for (size_t i = 0; i < cols.size(); i++) {
      const auto &column = cols.at(i);
      this->column_names_.push_back(column.column_name());
      this->column_types_.push_back(column.column_type());
      if (column.HasConstraint(sqlast::ColumnConstraint::Type::PRIMARY_KEY)) {
        this->keys_.push_back(i);
      }
    }
  }

  Schema(const std::vector<std::string> &column_names,
         const std::vector<sqlast::ColumnDefinition::Type> &column_types,
         const std::vector<ColumnID> &keys)
      : column_names_(column_names), column_types_(column_types), keys_(keys) {
    assert(column_names.size() == column_types.size());
  }

  // Not copyable or movable, we will only pass this around by reference or
  // pointer.
  Schema(const Schema &) = delete;
  Schema &operator=(const Schema &) = delete;
  Schema(const Schema &&) = delete;
  Schema &operator=(const Schema &&) = delete;

  // Equality is pointer equality.
  bool operator==(const Schema &other) const { return this == &other; }
  bool operator!=(const Schema &other) const { return this != &other; }

  // Accessors.
  const std::vector<std::string> &column_names() const {
    return this->column_names_;
  }
  const std::vector<sqlast::ColumnDefinition::Type> &column_types() const {
    return this->column_types_;
  }
  const std::vector<ColumnID> &keys() const { return this->keys_; }
  sqlast::ColumnDefinition::Type TypeOf(size_t i) const {
    if (i >= this->column_types_.size()) {
      LOG(FATAL) << "Schema: index out of bounds " << i << " / "
                 << this->column_types_.size();
    }
    return this->column_types_.at(i);
  }
  const std::string &NameOf(size_t i) const {
    if (i >= this->column_names_.size()) {
      LOG(FATAL) << "Schema: index out of bounds " << i << " / "
                 << this->column_names_.size();
    }
    return this->column_names_.at(i);
  }

  size_t Size() const { return this->column_names_.size(); }

 private:
  std::vector<std::string> column_names_;
  std::vector<sqlast::ColumnDefinition::Type> column_types_;
  std::vector<ColumnID> keys_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_SCHEMA_H_
