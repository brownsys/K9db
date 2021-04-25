#include "pelton/dataflow/ops/project.h"

#include <algorithm>
#include <memory>
#include <tuple>
#include <utility>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"
#include "pelton/sqlast/ast.h"

#define OPERATION_RIGHT_LITERAL_MACRO(OP)                                      \
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
// OPERATION_RIGHT_LITERAL_MACRO

#define OPERATION_RIGHT_COLUMN_MACRO(OP)                                      \
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
// OPERATION_RIGHT_COLUMN_MACRO

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
  std::vector<std::string> out_column_names = {};
  std::vector<sqlast::ColumnDefinition::Type> out_column_types = {};
  std::vector<ColumnID> input_keys = this->input_schemas_.at(0).keys();
  std::vector<ColumnID> out_keys = {};

  // Get cids for determining whether input keys are being projected or not.
  std::vector<ColumnID> cids;
  for (const auto &[column_name, left_operand, op, right_operand, metadata] :
       this->projections_) {
    switch (metadata) {
      case Metadata::COLUMN:
        cids.push_back(left_operand);
        break;
      default:
        continue;
    }
  }
  // If the input key set is a subset of the projected columns only then form an
  // output keyset. Else do not assign keys for the output schema. Because the
  // subset of input keycols can no longer uniqely identify records. This is
  // only for semantic purposes as, as of now, this does not have an effect on
  // the materialized view.
  if (EnclosedKeyCols(input_keys, cids)) {
    for (auto ik : input_keys) {
      auto it = std::find(cids.begin(), cids.end(), ik);
      if (it != cids.end()) {
        out_keys.push_back(std::distance(cids.begin(), it));
      }
    }
  }

  // Obtain column name and types
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
      case Metadata::OPERATION_RIGHT_LITERAL:
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
      case Metadata::OPERATION_RIGHT_COLUMN:
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
    Record out_record{this->output_schema_};
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
        case Metadata::OPERATION_RIGHT_LITERAL:
          switch (op) {
            case Operation::MINUS:
              OPERATION_RIGHT_LITERAL_MACRO(-)
              break;
            case Operation::PLUS:
              OPERATION_RIGHT_LITERAL_MACRO(+)
              break;
            default:
              LOG(FATAL) << "Unsupported arithmetic operation in project";
          }
          break;
        case Metadata::OPERATION_RIGHT_COLUMN:
          switch (op) {
            case Operation::MINUS:
              OPERATION_RIGHT_COLUMN_MACRO(-)
              break;
            case Operation::PLUS:
              OPERATION_RIGHT_COLUMN_MACRO(+)
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
