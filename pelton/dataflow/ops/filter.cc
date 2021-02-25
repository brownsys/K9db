#include "pelton/dataflow/ops/filter.h"

#include <tuple>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"
#include "pelton/sqlast/ast.h"

// The value in v must be of the same type as the corresponding one in the
// record schema.
// TODO(babamn): do this ahead of time at operator construction.
#define RECORD_VALUE_COMPARE_MACRO(v, col, OP)                    \
  if (record.schema().TypeOf(col) != Record::TypeOfVariant(v)) {  \
    LOG(FATAL) << "Type mistmatch in filter value";               \
  }                                                               \
  switch (record.schema().TypeOf(col)) {                          \
    case sqlast::ColumnDefinition::Type::INT:                     \
      if (!(record.GetInt(col) OP std::get<int64_t>(v))) {        \
        return false;                                             \
      }                                                           \
      break;                                                      \
    case sqlast::ColumnDefinition::Type::UINT:                    \
      if (!(record.GetUInt(col) OP std::get<uint64_t>(v))) {      \
        return false;                                             \
      }                                                           \
      break;                                                      \
    case sqlast::ColumnDefinition::Type::TEXT:                    \
      if (!(record.GetString(col) OP std::get<std::string>(v))) { \
        return false;                                             \
      }                                                           \
      break;                                                      \
    default:                                                      \
      LOG(FATAL) << "Unsupported data type in filter operator";   \
  }                                                               \
  break;
// RECORD_VALUE_COMPARE_MACRO

namespace pelton {
namespace dataflow {

void FilterOperator::ComputeOutputSchema() {
  this->output_schema_ = this->input_schemas_.at(0);
}

bool FilterOperator::Process(NodeIndex source,
                             const std::vector<Record> &records,
                             std::vector<Record> *output) {
  for (const Record &record : records) {
    if (this->Accept(record)) {
      output->push_back(record.Copy());
    }
  }
  return true;
}

bool FilterOperator::Accept(const Record &record) const {
  for (const auto &[v, col, op] : this->ops_) {
    switch (op) {
      case Operation::LESS_THAN:
        RECORD_VALUE_COMPARE_MACRO(v, col, <);

      case Operation::LESS_THAN_OR_EQUAL:
        RECORD_VALUE_COMPARE_MACRO(v, col, <=);

      case Operation::GREATER_THAN:
        RECORD_VALUE_COMPARE_MACRO(v, col, >);

      case Operation::GREATER_THAN_OR_EQUAL:
        RECORD_VALUE_COMPARE_MACRO(v, col, >=);

      case Operation::EQUAL:
        RECORD_VALUE_COMPARE_MACRO(v, col, ==);

      case Operation::NOT_EQUAL:
        RECORD_VALUE_COMPARE_MACRO(v, col, !=);

      default:
        LOG(FATAL) << "Unsupported operation in filter";
    }
  }
  return true;
}

}  // namespace dataflow
}  // namespace pelton
