#include "dataflow/schema.h"

#include "absl/strings/match.h"

namespace dataflow {

DataType StringToDataType(const std::string &type_name) {
  if (absl::EqualsIgnoreCase(type_name, "int")) {
    return dataflow::DataType::kInt;
  } else if (absl::StartsWithIgnoreCase(type_name, "varchar")) {
    return dataflow::DataType::kText;
  } else {
    throw "Unsupported datatype!";
  }
}

Schema::Schema(std::vector<DataType> columns)
    : Schema(columns, names_) {
}

Schema::Schema(std::vector<DataType> columns, std::vector<std::string> names)
    : types_(columns), names_(names) {
  num_inline_columns_ = 0;
  num_pointer_columns_ = 0;
  for (auto t : columns) {
    if (is_inlineable(t)) {
      true_indices_.push_back(std::make_pair(true, num_inline_columns_));
      num_inline_columns_++;
    } else {
      true_indices_.push_back(std::make_pair(false, num_pointer_columns_));
      num_pointer_columns_++;
    }
  }
}

Schema::Schema(const shards::sqlast::CreateTable &table_description) {
  auto pk = shards::sqlast::ColumnConstraint::Type::PRIMARY_KEY;

  this->num_inline_columns_ = 0;
  this->num_pointer_columns_ = 0;
  for (const auto &column : table_description.GetColumns()) {
    DataType t = StringToDataType(column.column_type());
    this->types_.push_back(t);
    this->names_.push_back(column.column_name());
    if (column.HasConstraint(pk)) {
      this->key_columns_.push_back(
          this->num_inline_columns_ + this->num_pointer_columns_);
    }
    if (is_inlineable(t)) {
      true_indices_.push_back(std::make_pair(true, num_inline_columns_));
      num_inline_columns_++;
    } else {
      true_indices_.push_back(std::make_pair(false, num_pointer_columns_));
      num_pointer_columns_++;
    }
  }
}

}  // namespace dataflow
