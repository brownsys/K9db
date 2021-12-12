#include "pelton/dataflow/schema.h"

#include <algorithm>

namespace pelton {
namespace dataflow {

// SchemaRef.
const std::vector<std::string> &SchemaRef::column_names() const {
  return this->ptr_->column_names;
}
const std::vector<sqlast::ColumnDefinition::Type> &SchemaRef::column_types()
    const {
  return this->ptr_->column_types;
}
const std::vector<ColumnID> &SchemaRef::keys() const {
  return this->ptr_->keys;
}
size_t SchemaRef::size() const {
  if (this->ptr_ == nullptr) {
    return 0;
  } else {
    return this->ptr_->column_names.size();
  }
}

// Accessor by index.
sqlast::ColumnDefinition::Type SchemaRef::TypeOf(size_t i) const {
  if (i >= this->ptr_->column_types.size()) {
    LOG(FATAL) << "Schema " << *this << ": index out of bounds " << i << " / "
               << this->ptr_->column_types.size();
  }
  return this->ptr_->column_types.at(i);
}
const std::string &SchemaRef::NameOf(size_t i) const {
  if (i >= this->ptr_->column_names.size()) {
    LOG(FATAL) << "Schema " << *this << ": index out of bounds " << i << " / "
               << this->ptr_->column_names.size();
  }
  return this->ptr_->column_names.at(i);
}

// Accessor by column name.
size_t SchemaRef::IndexOf(const std::string &column_name) const {
  auto it = find(this->ptr_->column_names.cbegin(),
                 this->ptr_->column_names.cend(), column_name);
  if (it == this->ptr_->column_names.cend()) {
    LOG(FATAL) << "Schema " << *this << ": column does not exist "
               << column_name;
  }
  return it - this->ptr_->column_names.cbegin();
}

// Printing a record to an output stream (e.g. std::cout).
std::ostream &operator<<(std::ostream &os,
                         const pelton::dataflow::SchemaRef &schema) {
  CHECK_NOTNULL(schema.ptr_);
  const auto &keys = schema.ptr_->keys;
  os << "|";
  for (unsigned i = 0; i < schema.size(); ++i) {
    os << schema.NameOf(i) << "(" << schema.TypeOf(i) << "";
    if (std::find(keys.cbegin(), keys.cend(), i) != keys.cend()) {
      os << ",KEY";
    }
    os << ")|";
  }
  return os;
}

// SchemaFactory.
std::list<SchemaData> SchemaFactory::SCHEMAS = {};

SchemaRef SchemaFactory::Create(const sqlast::CreateTable &table) {
  // Deduce schema from the Create statement.
  std::vector<std::string> column_names;
  std::vector<sqlast::ColumnDefinition::Type> column_types;
  std::vector<ColumnID> keys;
  const auto &cols = table.GetColumns();
  for (size_t i = 0; i < cols.size(); i++) {
    const auto &column = cols.at(i);
    column_names.push_back(column.column_name());
    column_types.push_back(column.column_type());
    if (column.HasConstraint(sqlast::ColumnConstraint::Type::PRIMARY_KEY)) {
      keys.push_back(i);
    }
  }
  // Create or re-use a SchemaRef for the deduced schema.
  return SchemaFactory::Create(column_names, column_types, keys);
}

SchemaRef SchemaFactory::Create(
    const std::vector<std::string> &column_names,
    const std::vector<sqlast::ColumnDefinition::Type> &column_types,
    const std::vector<ColumnID> &keys) {
  // Reuse SchemaData if found.
  for (const SchemaData &stored : SchemaFactory::SCHEMAS) {
    if (stored.Is(column_names, column_types, keys)) {
      return SchemaRef(&stored);
    }
  }
  // Not found: create a new SchemaData.
  SchemaFactory::SCHEMAS.emplace_back(column_names, column_types, keys);
  return SchemaRef(&SchemaFactory::SCHEMAS.back());
}

// Special hardcoded schemas.
SchemaRef SchemaFactory::MEMORY_SIZE_SCHEMA = SchemaFactory::Create(
    std::vector<std::string>{"Flow", "Operator ID", "Size(KB)"},
    std::vector<sqlast::ColumnDefinition::Type>{
        sqlast::ColumnDefinition::Type::TEXT,
        sqlast::ColumnDefinition::Type::TEXT,
        sqlast::ColumnDefinition::Type::UINT},
    std::vector<ColumnID>{0, 1});

SchemaRef SchemaFactory::FLOW_DEBUG_SCHEMA = SchemaFactory::Create(
    std::vector<std::string>{"ID", "Type", "Output Schema", "Children",
                             "Parents", "Info"},
    std::vector<sqlast::ColumnDefinition::Type>{
        sqlast::ColumnDefinition::Type::UINT,
        sqlast::ColumnDefinition::Type::TEXT,
        sqlast::ColumnDefinition::Type::TEXT,
        sqlast::ColumnDefinition::Type::TEXT,
        sqlast::ColumnDefinition::Type::TEXT,
        sqlast::ColumnDefinition::Type::TEXT},
    std::vector<ColumnID>{0});

SchemaRef SchemaFactory::NUM_SHARDS_SCHEMA =
    SchemaFactory::Create(std::vector<std::string>{"Shard Kind", "Count"},
                          std::vector<sqlast::ColumnDefinition::Type>{
                              sqlast::ColumnDefinition::Type::TEXT,
                              sqlast::ColumnDefinition::Type::UINT},
                          std::vector<ColumnID>{0});

}  // namespace dataflow
}  // namespace pelton
