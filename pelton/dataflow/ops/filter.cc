#include "pelton/dataflow/ops/filter.h"

#include <tuple>
#include <utility>

#include "glog/logging.h"

// The value in v must be of the same type as the corresponding one in the
// record schema.
#define COLUMN_VALUE_CMP_MACRO(col, v, OP)                      \
  if (record.IsNull(col)) {                                     \
    return false;                                               \
  }                                                             \
  switch (record.schema().TypeOf(col)) {                        \
    case sqlast::ColumnDefinition::Type::INT:                   \
      if (!(record.GetInt(col) OP v.GetInt())) {                \
        return false;                                           \
      }                                                         \
      break;                                                    \
    case sqlast::ColumnDefinition::Type::UINT:                  \
      if (!(record.GetUInt(col) OP v.GetUInt())) {              \
        return false;                                           \
      }                                                         \
      break;                                                    \
    case sqlast::ColumnDefinition::Type::TEXT:                  \
      if (!(record.GetString(col) OP v.GetString())) {          \
        return false;                                           \
      }                                                         \
      break;                                                    \
    case sqlast::ColumnDefinition::Type::DATETIME:              \
      if (!(record.GetDateTime(col) OP v.GetString())) {        \
        return false;                                           \
      }                                                         \
      break;                                                    \
    default:                                                    \
      LOG(FATAL) << "Unsupported data type in filter operator"; \
  }                                                             \
  // COLUMN_VALUE_COMPARE_MACRO

#define COLUMN_COLUMN_CMP_MACRO(col1, col2, OP)                       \
  if (record.schema().TypeOf(col1) != record.schema().TypeOf(col2)) { \
    LOG(FATAL) << "Type mistmatch in filter column";                  \
  }                                                                   \
  if (record.IsNull(col1) && record.IsNull(col2)) {                   \
    return true;                                                      \
  } else if (record.IsNull(col1) || record.IsNull(col2)) {            \
    return false;                                                     \
  }                                                                   \
  switch (record.schema().TypeOf(col1)) {                             \
    case sqlast::ColumnDefinition::Type::INT:                         \
      if (!(record.GetInt(col1) OP record.GetInt(col2))) {            \
        return false;                                                 \
      }                                                               \
      break;                                                          \
    case sqlast::ColumnDefinition::Type::UINT:                        \
      if (!(record.GetUInt(col1) OP record.GetUInt(col2))) {          \
        return false;                                                 \
      }                                                               \
      break;                                                          \
    case sqlast::ColumnDefinition::Type::TEXT:                        \
      if (!(record.GetString(col1) OP record.GetString(col2))) {      \
        return false;                                                 \
      }                                                               \
      break;                                                          \
    case sqlast::ColumnDefinition::Type::DATETIME:                    \
      if (!(record.GetDateTime(col1) OP record.GetDateTime(col2))) {  \
        return false;                                                 \
      }                                                               \
      break;                                                          \
    default:                                                          \
      LOG(FATAL) << "Unsupported data type in filter operator";       \
  }                                                                   \
  // COLUMN_VALUE_COMPARE_MACRO

#define GENERIC_CMP_MACRO(operation, OP)                                     \
  if (operation.is_column()) {                                               \
    COLUMN_COLUMN_CMP_MACRO(operation.left(), operation.right_column(), OP); \
  } else {                                                                   \
    COLUMN_VALUE_CMP_MACRO(operation.left(), operation.right_value(), OP);   \
  }                                                                          \
  break
// GENERIC_COMPARE_MACRO

namespace pelton {
namespace dataflow {

void FilterOperator::ComputeOutputSchema() {
  this->output_schema_ = this->input_schemas_.at(0);
}

std::vector<Record> FilterOperator::Process(NodeIndex source,
                                            std::vector<Record> &&records,
                                            const Promise &promise) {
  std::vector<Record> output;
  output.reserve(records.size());
  for (Record &record : records) {
    if (this->Accept(record)) {
      output.push_back(std::move(record));
    }
  }
  return output;
}

bool FilterOperator::Accept(const Record &record) const {
  for (const auto &operation : this->ops_) {
    switch (operation.op()) {
      case Operation::LESS_THAN:
        GENERIC_CMP_MACRO(operation, <);

      case Operation::LESS_THAN_OR_EQUAL:
        GENERIC_CMP_MACRO(operation, <=);

      case Operation::GREATER_THAN:
        GENERIC_CMP_MACRO(operation, >);

      case Operation::GREATER_THAN_OR_EQUAL:
        GENERIC_CMP_MACRO(operation, >=);

      case Operation::EQUAL:
        GENERIC_CMP_MACRO(operation, ==);

      case Operation::NOT_EQUAL:
        GENERIC_CMP_MACRO(operation, !=);

      case Operation::IS_NULL:
        if (!record.IsNull(operation.left())) {
          return false;
        }
        break;

      case Operation::IS_NOT_NULL:
        if (record.IsNull(operation.left())) {
          return false;
        }
        break;

      default:
        LOG(FATAL) << "Unsupported operation in filter";
    }
  }
  return true;
}

std::unique_ptr<Operator> FilterOperator::Clone() const {
  auto clone = std::make_unique<FilterOperator>();
  clone->ops_ = this->ops_;
  return clone;
}

}  // namespace dataflow
}  // namespace pelton
