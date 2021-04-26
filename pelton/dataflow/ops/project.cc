#include "pelton/dataflow/ops/project.h"

#include <algorithm>
#include <memory>
#include <tuple>
#include <utility>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"
#include "pelton/sqlast/ast.h"

#define ARITHMETIC_WITH_LITERAL_MACRO(OP)                                      \
  {                                                                            \
    switch (this->output_schema_.TypeOf(i)) {                                  \
      case sqlast::ColumnDefinition::Type::UINT:                               \
        out_record.SetUInt(                                                    \
            record.GetUInt(left_operand) OP std::get<uint64_t>(right_operand), \
            i);                                                                \
        break;                                                                 \
      case sqlast::ColumnDefinition::Type::INT:                                \
        switch (record.schema().TypeOf(left_operand)) {                        \
          case sqlast::ColumnDefinition::Type::UINT:                           \
            out_record.SetInt(record.GetUInt(left_operand)                     \
                                  OP std::get<uint64_t>(right_operand),        \
                              i);                                              \
            break;                                                             \
          case sqlast::ColumnDefinition::Type::INT:                            \
            out_record.SetInt(record.GetInt(left_operand)                      \
                                  OP std::get<int64_t>(right_operand),         \
                              i);                                              \
            break;                                                             \
          default:                                                             \
            LOG(FATAL) << "Unsupported column type for operation in project";  \
        }                                                                      \
        break;                                                                 \
      default:                                                                 \
        LOG(FATAL) << "Unsupported arithmetic operation in project";           \
    }                                                                          \
  }
// In above macro an edge case is being handled for (uint MINUS uint)
// ARITHMETIC_WITH_LITERAL_MACRO

#define ARITHMETIC_WITH_COLUMN_MACRO(OP)                                      \
  {                                                                           \
    switch (this->output_schema_.TypeOf(i)) {                                 \
      case sqlast::ColumnDefinition::Type::UINT:                              \
        out_record.SetUInt(record.GetUInt(left_operand) OP record.GetUInt(    \
                               std::get<uint64_t>(right_operand)),            \
                           i);                                                \
        break;                                                                \
      case sqlast::ColumnDefinition::Type::INT:                               \
        switch (record.schema().TypeOf(left_operand)) {                       \
          case sqlast::ColumnDefinition::Type::UINT:                          \
            out_record.SetInt(record.GetUInt(left_operand) OP record.GetUInt( \
                                  std::get<uint64_t>(right_operand)),         \
                              i);                                             \
            break;                                                            \
          case sqlast::ColumnDefinition::Type::INT:                           \
            out_record.SetInt(record.GetInt(left_operand) OP record.GetInt(   \
                                  std::get<uint64_t>(right_operand)),         \
                              i);                                             \
            break;                                                            \
          default:                                                            \
            LOG(FATAL) << "Unsupported column type for operation in project"; \
        }                                                                     \
        break;                                                                \
      default:                                                                \
        LOG(FATAL) << "Unsupported arithmetic operation in project";          \
    }                                                                         \
  }
// In above macro an edge case is being handled for (uint MINUS uint)
// ARITHMETIC_WITH_COLUMN_MACRO

namespace pelton {
namespace dataflow {

bool ProjectOperator::EnclosedKeyCols(
    const std::vector<ColumnID> &input_keycols,
    const std::vector<ColumnID> &cids) const {
  for (const auto &keycol : input_keycols) {
    bool is_present = false;
    for (const auto &cid : cids)
      if (keycol == cid) is_present = true;

    // at least one key column of the composite key is not being projected
    if (!is_present) return false;
  }
  // all input key columns are present in the projected schema
  return true;
}

inline sqlast::ColumnDefinition::Type GetLiteralType(
    const Record::DataVariant &literal) {
  switch (literal.index()) {
    case 1:
      return sqlast::ColumnDefinition::Type::UINT;
    case 2:
      return sqlast::ColumnDefinition::Type::INT;
    default:
      LOG(FATAL) << "Unsupported literal type in project";
  }
}

void ProjectOperator::ComputeOutputSchema() {
  // If keys are not provided, we can compute them ourselves by checking
  // if the primary key survives the projection.
  std::vector<ColumnID> out_keys = this->keys_;
  if (!this->has_keys_) {
    out_keys = {};
    for (ColumnID key : this->input_schemas_.at(0).keys()) {
      bool found_key = false;
      for (size_t i = 0; i < this->projections_.size(); i++) {
        const auto &[column_name, left_operand, op, right_operand, metadata] =
            this->projections_.at(i);
        if (metadata == Metadata::COLUMN && left_operand == key) {
          out_keys.push_back(i);
          found_key = true;
          break;
        }
      }
      if (!found_key) {
        out_keys.clear();
        break;
      }
    }
  }

  // Obtain column name and types
  std::vector<std::string> out_column_names = {};
  std::vector<sqlast::ColumnDefinition::Type> out_column_types = {};
  for (const auto &[column_name, left_operand, op, right_operand, metadata] :
       this->projections_) {
    // Obtain types and simultaneously sanity check for type compatibility
    // in case of airthmetic operations
    switch (metadata) {
      case Metadata::COLUMN:
        out_column_types.push_back(
            this->input_schemas_.at(0).TypeOf(left_operand));
        break;
      case Metadata::LITERAL:
        out_column_types.push_back(GetLiteralType(right_operand));
        break;
      case Metadata::ARITHMETIC_WITH_LITERAL:
        if (this->input_schemas_.at(0).TypeOf(left_operand) !=
            GetLiteralType(right_operand))
          LOG(FATAL) << "Column and literal do not have same types for "
                        "arithmetic operation in projection";
        if (op == Operation::MINUS) {
          // uint MINUS uint would result in an int
          out_column_types.push_back(sqlast::ColumnDefinition::Type::INT);
        } else {
          out_column_types.push_back(
              this->input_schemas_.at(0).TypeOf(left_operand));
        }
        break;
      case Metadata::ARITHMETIC_WITH_COLUMN:
        if (this->input_schemas_.at(0).TypeOf(left_operand) !=
            this->input_schemas_.at(0).TypeOf(
                std::get<uint64_t>(right_operand)))
          LOG(FATAL) << "Columns do not have same types for arithmetic "
                        "operation in projection";
        if (op == Operation::MINUS) {
          // uint MINUS uint would result in an int
          out_column_types.push_back(sqlast::ColumnDefinition::Type::INT);
        } else {
          out_column_types.push_back(
              this->input_schemas_.at(0).TypeOf(left_operand));
        }
    }
    out_column_names.push_back(column_name);
  }

  this->output_schema_ =
      SchemaFactory::Create(out_column_names, out_column_types, out_keys);
}

bool ProjectOperator::Process(NodeIndex source,
                              const std::vector<Record> &records,
                              std::vector<Record> *output) {
  for (const Record &record : records) {
    Record out_record{this->output_schema_, record.IsPositive()};
    for (size_t i = 0; i < this->projections_.size(); i++) {
      if (record.IsNull(i)) {
        out_record.SetNull(true, i);
        continue;
      }
      const auto &[column_name, left_operand, op, right_operand, metadata] =
          this->projections_.at(i);
      switch (metadata) {
        case Metadata::COLUMN:
          switch (this->output_schema_.TypeOf(i)) {
            case sqlast::ColumnDefinition::Type::UINT:
              out_record.SetUInt(record.GetUInt(left_operand), i);
              break;
            case sqlast::ColumnDefinition::Type::INT:
              out_record.SetInt(record.GetInt(left_operand), i);
              break;
            case sqlast::ColumnDefinition::Type::TEXT:
              out_record.SetString(
                  std::make_unique<std::string>(record.GetString(left_operand)),
                  i);
              break;
            default:
              LOG(FATAL) << "Unsupported type in project";
          }
          break;
        case Metadata::LITERAL:
          switch (right_operand.index()) {
            case 1:
              out_record.SetUInt(std::get<uint64_t>(right_operand), i);
              break;
            case 2:
              out_record.SetInt(std::get<int64_t>(right_operand), i);
              break;
            default:
              LOG(FATAL) << "Unsupported literal type";
          }
          break;
        case Metadata::ARITHMETIC_WITH_LITERAL:
          switch (op) {
            case Operation::MINUS:
              ARITHMETIC_WITH_LITERAL_MACRO(-)
              break;
            case Operation::PLUS:
              ARITHMETIC_WITH_LITERAL_MACRO(+)
              break;
            default:
              LOG(FATAL) << "Unsupported arithmetic operation in project";
          }
          break;
        case Metadata::ARITHMETIC_WITH_COLUMN:
          switch (op) {
            case Operation::MINUS:
              ARITHMETIC_WITH_COLUMN_MACRO(-)
              break;
            case Operation::PLUS:
              ARITHMETIC_WITH_COLUMN_MACRO(+)
              break;
            default:
              LOG(FATAL) << "Unsupported arithmetic operation in project";
          }
          break;
      }
    }
    output->push_back(std::move(out_record));
  }
  return true;
}

}  // namespace dataflow
}  // namespace pelton
