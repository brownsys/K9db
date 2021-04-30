#include "pelton/dataflow/ops/project.h"

#include <algorithm>
#include <memory>
#include <tuple>
#include <utility>

#include "glog/logging.h"
#include "pelton/dataflow/record.h"
#include "pelton/sqlast/ast.h"

#define HANDLE_NULL_MACRO_COLUMN(col, other_col, OP)            \
  if (record.IsNull(col)) {                                     \
    switch (this->output_schema_.TypeOf(i)) {                   \
      case sqlast::ColumnDefinition::Type::UINT:                \
        out_record.SetUInt(record.GetUInt(other_col), i);       \
        break;                                                  \
      case sqlast::ColumnDefinition::Type::INT: {               \
        bool is_int = record.schema().TypeOf(other_col) ==      \
                      sqlast::ColumnDefinition::Type::INT;      \
        if (is_int) {                                           \
          out_record.SetInt(OP record.GetInt(other_col), i);    \
        } else {                                                \
          out_record.SetInt(OP record.GetUInt(other_col), i);   \
        }                                                       \
        break;                                                  \
      }                                                         \
      default:                                                  \
        LOG(FATAL) << "Unsupported arithmetic type in project"; \
    }                                                           \
  }

#define HANDLE_NULL_MACRO_LITERAL(col, literal, OP)                         \
  if (record.IsNull(col)) {                                                 \
    switch (this->output_schema_.TypeOf(i)) {                               \
      case sqlast::ColumnDefinition::Type::UINT:                            \
        out_record.SetUInt(std::get<uint64_t>(literal), i);                 \
        break;                                                              \
      case sqlast::ColumnDefinition::Type::INT: {                           \
        bool is_int =                                                       \
            GetLiteralType(literal) == sqlast::ColumnDefinition::Type::INT; \
        if (is_int) {                                                       \
          out_record.SetInt(OP std::get<int64_t>(literal), i);              \
        } else {                                                            \
          out_record.SetInt(OP std::get<uint64_t>(literal), i);             \
        }                                                                   \
        break;                                                              \
      }                                                                     \
      default:                                                              \
        LOG(FATAL) << "Unsupported arithmetic type in project";             \
    }                                                                       \
  }

#define LEFT_ARITHMETIC_WITH_LITERAL_MACRO(OP)                                \
  HANDLE_NULL_MACRO_LITERAL(left, right, OP)                                  \
  /* NOLINTNEXTLINE */                                                        \
  else {                                                                      \
    switch (this->output_schema_.TypeOf(i)) {                                 \
      case sqlast::ColumnDefinition::Type::UINT:                              \
        out_record.SetUInt(record.GetUInt(left) OP std::get<uint64_t>(right), \
                           i);                                                \
        break;                                                                \
      case sqlast::ColumnDefinition::Type::INT: {                             \
        int64_t result;                                                       \
        bool lint = record.schema().TypeOf(left) ==                           \
                    sqlast::ColumnDefinition::Type::INT;                      \
        bool rint =                                                           \
            GetLiteralType(right) == sqlast::ColumnDefinition::Type::INT;     \
        if (lint && rint) {                                                   \
          result = record.GetInt(left) OP std::get<int64_t>(right);           \
        } else if (lint) {                                                    \
          result = record.GetInt(left) OP std::get<uint64_t>(right);          \
        } else if (rint) {                                                    \
          result = record.GetUInt(left) OP std::get<int64_t>(right);          \
        } else {                                                              \
          result = record.GetUInt(left) OP std::get<uint64_t>(right);         \
        }                                                                     \
        out_record.SetInt(result, i);                                         \
        break;                                                                \
      }                                                                       \
      default:                                                                \
        LOG(FATAL) << "Unsupported arithmetic type in project";               \
    }                                                                         \
  }

// In above macro an edge case is being handled for (uint MINUS uint)
// LEFT_ARITHMETIC_WITH_LITERAL_MACRO

#define RIGHT_ARITHMETIC_WITH_LITERAL_MACRO(OP)                               \
  HANDLE_NULL_MACRO_LITERAL(right, left, +)                                   \
  /* NOLINTNEXTLINE */                                                        \
  else {                                                                      \
    switch (this->output_schema_.TypeOf(i)) {                                 \
      case sqlast::ColumnDefinition::Type::UINT:                              \
        out_record.SetUInt(std::get<uint64_t>(left) OP record.GetUInt(right), \
                           i);                                                \
        break;                                                                \
      case sqlast::ColumnDefinition::Type::INT: {                             \
        int64_t result;                                                       \
        bool lint =                                                           \
            GetLiteralType(left) == sqlast::ColumnDefinition::Type::INT;      \
        bool rint = record.schema().TypeOf(right) ==                          \
                    sqlast::ColumnDefinition::Type::INT;                      \
        if (lint && rint) {                                                   \
          result = std::get<int64_t>(left) OP record.GetInt(right);           \
        } else if (lint) {                                                    \
          result = std::get<int64_t>(left) OP record.GetUInt(right);          \
        } else if (rint) {                                                    \
          result = std::get<uint64_t>(left) OP record.GetInt(right);          \
        } else {                                                              \
          result = std::get<uint64_t>(left) OP record.GetUInt(right);         \
        }                                                                     \
        out_record.SetInt(result, i);                                         \
        break;                                                                \
      }                                                                       \
      default:                                                                \
        LOG(FATAL) << "Unsupported arithmetic type in project";               \
    }                                                                         \
  }
// In above macro an edge case is being handled for (uint MINUS uint)
// RIGHT_ARITHMETIC_WITH_LITERAL_MACRO

#define ARITHMETIC_WITH_COLUMN_MACRO(OP)                                      \
  HANDLE_NULL_MACRO_COLUMN(left, right, OP)                                   \
  /* NOLINTNEXTLINE */                                                        \
  else HANDLE_NULL_MACRO_COLUMN(right, left, +) /* NOLINTNEXTLINE */          \
      else {                                                                  \
    switch (this->output_schema_.TypeOf(i)) {                                 \
      case sqlast::ColumnDefinition::Type::UINT:                              \
        out_record.SetUInt(record.GetUInt(left) OP record.GetUInt(right), i); \
        break;                                                                \
      case sqlast::ColumnDefinition::Type::INT: {                             \
        int64_t result;                                                       \
        bool lint = record.schema().TypeOf(left) ==                           \
                    sqlast::ColumnDefinition::Type::INT;                      \
        bool rint = record.schema().TypeOf(right) ==                          \
                    sqlast::ColumnDefinition::Type::INT;                      \
        if (lint && rint) {                                                   \
          result = record.GetInt(left) OP record.GetInt(right);               \
        } else if (lint) {                                                    \
          result = record.GetInt(left) OP record.GetUInt(right);              \
        } else if (rint) {                                                    \
          result = record.GetUInt(left) OP record.GetInt(right);              \
        } else {                                                              \
          result = record.GetUInt(left) OP record.GetUInt(right);             \
        }                                                                     \
        out_record.SetInt(result, i);                                         \
        break;                                                                \
      }                                                                       \
      default:                                                                \
        LOG(FATAL) << "Unsupported arithmetic type in project";               \
    }                                                                         \
  }
// In above macro an edge case is being handled for (uint MINUS uint)
// ARITHMETIC_WITH_COLUMN_MACRO

namespace pelton {
namespace dataflow {

namespace {

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

}  // namespace

void ProjectOperator::ComputeOutputSchema() {
  // If keys are not provided, we can compute them ourselves by checking
  // if the primary key survives the projection.
  std::vector<ColumnID> out_keys = {};
  for (ColumnID key : this->input_schemas_.at(0).keys()) {
    bool found_key = false;
    for (size_t i = 0; i < this->projections_.size(); i++) {
      const auto &projection = this->projections_.at(i);
      if (projection.column() && projection.getColumn() == key) {
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

  // Obtain column name and types
  const SchemaRef &input = this->input_schemas_.at(0);
  std::vector<std::string> out_column_names = {};
  std::vector<sqlast::ColumnDefinition::Type> out_column_types = {};
  for (const auto &projection : this->projections_) {
    // Obtain types and simultaneously sanity check for type compatibility
    // in case of airthmetic operations
    if (projection.column()) {
      ColumnID column = projection.getColumn();
      const std::string &column_name = projection.getName();
      if (column_name.size() == 0) {
        out_column_names.push_back(input.NameOf(column));
      } else {
        out_column_names.push_back(column_name);
      }
      out_column_types.push_back(input.TypeOf(column));

    } else if (projection.literal()) {
      out_column_names.push_back(projection.getName());
      out_column_types.push_back(GetLiteralType(projection.getLiteral()));

    } else {
      out_column_names.push_back(projection.getName());

      sqlast::ColumnDefinition::Type result_type;
      if (projection.left_column() && projection.right_column()) {
        CHECK_EQ(input.TypeOf(projection.getLeftColumn()),
                 input.TypeOf(projection.getRightColumn()));
        result_type = input.TypeOf(projection.getLeftColumn());
      } else if (projection.left_column()) {
        Record::DataVariant literal = projection.getRightLiteral();
        CHECK_EQ(input.TypeOf(projection.getLeftColumn()),
                 GetLiteralType(literal));
        result_type = input.TypeOf(projection.getLeftColumn());
      } else {
        Record::DataVariant literal = projection.getLeftLiteral();
        CHECK_EQ(GetLiteralType(literal),
                 input.TypeOf(projection.getRightColumn()));
        result_type = input.TypeOf(projection.getRightColumn());
      }

      // uint - uint -> int.
      if (projection.getOperation() == Operation::MINUS) {
        result_type = sqlast::ColumnDefinition::Type::INT;
      }

      out_column_types.push_back(result_type);
    }
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
      const auto &projection = this->projections_.at(i);

      if (projection.column()) {
        // Project an existing column.
        ColumnID column = projection.getColumn();
        if (record.IsNull(column)) {
          out_record.SetNull(true, i);
          continue;
        }
        switch (this->output_schema_.TypeOf(i)) {
          case sqlast::ColumnDefinition::Type::UINT:
            out_record.SetUInt(record.GetUInt(column), i);
            break;
          case sqlast::ColumnDefinition::Type::INT:
            out_record.SetInt(record.GetInt(column), i);
            break;
          case sqlast::ColumnDefinition::Type::TEXT:
            out_record.SetString(
                std::make_unique<std::string>(record.GetString(column)), i);
          // TODO(malte): DATETIME should not be stored as a string,
          // see below
          case sqlast::ColumnDefinition::Type::DATETIME:
            out_record.SetDateTime(
                std::make_unique<std::string>(record.GetDateTime(column)), i);
            break;
          default:
            LOG(FATAL) << "Unsupported column type " << this->output_schema_.TypeOf(i) << " in project";
        }

      } else if (projection.literal()) {
        // Project a literal.
        Record::DataVariant literal = projection.getLiteral();
        switch (this->output_schema_.TypeOf(i)) {
          case sqlast::ColumnDefinition::Type::UINT:
            out_record.SetUInt(std::get<uint64_t>(literal), i);
            break;
          case sqlast::ColumnDefinition::Type::INT:
            out_record.SetInt(std::get<int64_t>(literal), i);
            break;
          default:
            LOG(FATAL) << "Unsupported literal type in project";
        }

      } else {
        if (projection.left_column() && projection.right_column()) {
          ColumnID left = projection.getLeftColumn();
          ColumnID right = projection.getRightColumn();
          switch (projection.getOperation()) {
            case Operation::MINUS: {
              ARITHMETIC_WITH_COLUMN_MACRO(-)
              break;
            }
            case Operation::PLUS: {
              ARITHMETIC_WITH_COLUMN_MACRO(+)
              break;
            }
            default:
              LOG(FATAL) << "Unsupported arithmetic operation in project";
          }

        } else if (projection.left_column()) {
          ColumnID left = projection.getLeftColumn();
          Record::DataVariant right = projection.getRightLiteral();
          switch (projection.getOperation()) {
            case Operation::MINUS: {
              LEFT_ARITHMETIC_WITH_LITERAL_MACRO(-)
              break;
            }
            case Operation::PLUS: {
              LEFT_ARITHMETIC_WITH_LITERAL_MACRO(+)
              break;
            }
            default:
              LOG(FATAL) << "Unsupported arithmetic operation in project";
          }

        } else {
          Record::DataVariant left = projection.getLeftLiteral();
          ColumnID right = projection.getRightColumn();
          switch (projection.getOperation()) {
            case Operation::MINUS: {
              RIGHT_ARITHMETIC_WITH_LITERAL_MACRO(-)
              break;
            }
            case Operation::PLUS: {
              RIGHT_ARITHMETIC_WITH_LITERAL_MACRO(+)
              break;
            }
            default:
              LOG(FATAL) << "Unsupported arithmetic operation in project";
          }
        }
      }
    }

    output->push_back(std::move(out_record));
  }

  return true;
}

}  // namespace dataflow
}  // namespace pelton
