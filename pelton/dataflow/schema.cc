#include "pelton/dataflow/schema.h"

namespace pelton {
namespace dataflow {

// Construct from a CREATE TABLE statement.
SchemaOwner::SchemaOwner(const sqlast::CreateTable &table) {
  this->ptr_ = new SchemaData();
  // Fill ptr_ with the schema data from the CREATE TABLE statement.
  const auto &cols = table.GetColumns();
  for (size_t i = 0; i < cols.size(); i++) {
    const auto &column = cols.at(i);
    this->ptr_->column_names.push_back(column.column_name());
    this->ptr_->column_types.push_back(column.column_type());
    if (column.HasConstraint(sqlast::ColumnConstraint::Type::PRIMARY_KEY)) {
      this->ptr_->keys.push_back(i);
    }
  }
}

// Accessors.
const std::vector<std::string> &SchemaOwner::column_names() const {
  return this->ptr_->column_names;
}
const std::vector<sqlast::ColumnDefinition::Type> &SchemaOwner::column_types()
    const {
  return this->ptr_->column_types;
}
const std::vector<ColumnID> &SchemaOwner::keys() const {
  return this->ptr_->keys;
}
size_t SchemaOwner::size() const { return this->ptr_->column_names.size(); }

// Accessor by index.
sqlast::ColumnDefinition::Type SchemaOwner::TypeOf(size_t i) const {
  if (i >= this->ptr_->column_types.size()) {
    LOG(FATAL) << "Schema: index out of bounds " << i << " / "
               << this->ptr_->column_types.size();
  }
  return this->ptr_->column_types.at(i);
}
const std::string &SchemaOwner::NameOf(size_t i) const {
  if (i >= this->ptr_->column_names.size()) {
    LOG(FATAL) << "Schema: index out of bounds " << i << " / "
               << this->ptr_->column_names.size();
  }
  return this->ptr_->column_names.at(i);
}

}  // namespace dataflow
}  // namespace pelton
