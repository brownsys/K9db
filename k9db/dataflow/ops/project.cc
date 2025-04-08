#include "k9db/dataflow/ops/project.h"

#include <algorithm>
#include <tuple>
#include <utility>

#include "glog/logging.h"
#include "k9db/dataflow/schema.h"

#define ARITHMETIC_WITH_RIGHT_LITERAL_MACRO(OP)                     \
  switch (this->output_schema_.TypeOf(i)) {                         \
    case sqlast::ColumnDefinition::Type::UINT: {                    \
      uint64_t lv = record.IsNull(left) ? 0 : record.GetUInt(left); \
      out_record.SetUInt(lv OP right.GetUInt(), i);                 \
      break;                                                        \
    }                                                               \
    case sqlast::ColumnDefinition::Type::INT: {                     \
      using CType = sqlast::ColumnDefinition::Type;                 \
      int64_t lv, rv;                                               \
      if (record.schema().TypeOf(left) == CType::INT) {             \
        lv = record.IsNull(left) ? 0 : record.GetInt(left);         \
      } else {                                                      \
        lv = record.IsNull(left) ? 0 : record.GetUInt(left);        \
      }                                                             \
      if (right.type() == sqlast::Value::Type::INT) {               \
        rv = right.GetInt();                                        \
      } else {                                                      \
        rv = right.GetUInt();                                       \
      }                                                             \
      out_record.SetInt(lv OP rv, i);                               \
      break;                                                        \
    }                                                               \
    default:                                                        \
      LOG(FATAL) << "Unsupported arithmetic type in project";       \
  }
// ARITHMETIC_WITH_RIGHT_LITERAL_MACRO

#define ARITHMETIC_WITH_LEFT_LITERAL_MACRO(OP)                        \
  switch (this->output_schema_.TypeOf(i)) {                           \
    case sqlast::ColumnDefinition::Type::UINT: {                      \
      uint64_t rv = record.IsNull(right) ? 0 : record.GetUInt(right); \
      out_record.SetUInt(left.GetUInt() OP rv, i);                    \
      break;                                                          \
    }                                                                 \
    case sqlast::ColumnDefinition::Type::INT: {                       \
      using CType = sqlast::ColumnDefinition::Type;                   \
      int64_t lv, rv;                                                 \
      if (left.type() == sqlast::Value::Type::INT) {                  \
        lv = left.GetInt();                                           \
      } else {                                                        \
        lv = left.GetUInt();                                          \
      }                                                               \
      if (record.schema().TypeOf(right) == CType::INT) {              \
        rv = record.IsNull(right) ? 0 : record.GetInt(right);         \
      } else {                                                        \
        rv = record.IsNull(right) ? 0 : record.GetUInt(right);        \
      }                                                               \
      out_record.SetInt(lv OP rv, i);                                 \
      break;                                                          \
    }                                                                 \
    default:                                                          \
      LOG(FATAL) << "Unsupported arithmetic type in project";         \
  }
// ARITHMETIC_WITH_LEFT_LITERAL_MACRO

#define ARITHMETIC_WITH_COLUMN_MACRO(OP)                                \
  if (record.IsNull(left) && record.IsNull(right)) {                    \
    out_record.SetNull(true, i);                                        \
  } else {                                                              \
    switch (this->output_schema_.TypeOf(i)) {                           \
      case sqlast::ColumnDefinition::Type::UINT: {                      \
        uint64_t lv = record.IsNull(left) ? 0 : record.GetUInt(left);   \
        uint64_t rv = record.IsNull(right) ? 0 : record.GetUInt(right); \
        out_record.SetUInt(lv OP rv, i);                                \
        break;                                                          \
      }                                                                 \
      case sqlast::ColumnDefinition::Type::INT: {                       \
        using CType = sqlast::ColumnDefinition::Type;                   \
        int64_t lv, rv;                                                 \
        if (record.schema().TypeOf(left) == CType::INT) {               \
          lv = record.IsNull(left) ? 0 : record.GetInt(left);           \
        } else {                                                        \
          lv = record.IsNull(left) ? 0 : record.GetUInt(left);          \
        }                                                               \
        if (record.schema().TypeOf(right) == CType::INT) {              \
          rv = record.IsNull(right) ? 0 : record.GetInt(right);         \
        } else {                                                        \
          rv = record.IsNull(right) ? 0 : record.GetUInt(right);        \
        }                                                               \
        out_record.SetInt(lv OP rv, i);                                 \
        break;                                                          \
      }                                                                 \
      default:                                                          \
        LOG(FATAL) << "Unsupported arithmetic type in project";         \
    }                                                                   \
  }
// In above macro an edge case is being handled for (uint MINUS uint) -> int
// ARITHMETIC_WITH_COLUMN_MACRO

namespace k9db {
namespace dataflow {

std::optional<ColumnID> ProjectOperator::ProjectColumn(ColumnID col) const {
  for (size_t i = 0; i < this->projections_.size(); i++) {
    const auto &projection = this->projections_.at(i);
    if (projection.column()) {
      ColumnID column = projection.getLeftColumn();
      if (column == col) {
        return i;
      }
    }
  }
  return {};
}
std::optional<ColumnID> ProjectOperator::UnprojectColumn(ColumnID col) const {
  const auto &projection = this->projections_.at(col);
  if (projection.column()) {
    return projection.getLeftColumn();
  }
  return {};
}

void ProjectOperator::ComputeOutputSchema() {
  // If keys are not provided, we can compute them ourselves by checking
  // if the primary key survives the projection.
  std::vector<ColumnID> out_keys = {};
  for (ColumnID key : this->input_schemas_.at(0).keys()) {
    bool found_key = false;
    for (size_t i = 0; i < this->projections_.size(); i++) {
      const auto &projection = this->projections_.at(i);
      if (projection.column() && projection.getLeftColumn() == key) {
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
      ColumnID column = projection.getLeftColumn();
      const std::string &column_name = projection.getName();
      if (column_name.size() == 0) {
        out_column_names.push_back(input.NameOf(column));
      } else {
        out_column_names.push_back(column_name);
      }
      out_column_types.push_back(input.TypeOf(column));

    } else if (projection.literal()) {
      out_column_names.push_back(projection.getName());
      out_column_types.push_back(projection.getLeftLiteral().ConvertType());

    } else {
      out_column_names.push_back(projection.getName());
      sqlast::ColumnDefinition::Type ltype, rtype;
      if (projection.left_column() && projection.right_column()) {
        ltype = input.TypeOf(projection.getLeftColumn());
        rtype = input.TypeOf(projection.getRightColumn());
      } else if (projection.left_column()) {
        ltype = input.TypeOf(projection.getLeftColumn());
        rtype = projection.getRightLiteral().ConvertType();
      } else {
        ltype = projection.getLeftLiteral().ConvertType();
        rtype = input.TypeOf(projection.getRightColumn());
      }

      // Cannot perform arithmetic operations over strings.
      CHECK_NE(ltype, sqlast::ColumnDefinition::Type::TEXT);
      CHECK_NE(ltype, sqlast::ColumnDefinition::Type::DATETIME);
      CHECK_NE(rtype, sqlast::ColumnDefinition::Type::TEXT);
      CHECK_NE(rtype, sqlast::ColumnDefinition::Type::DATETIME);

      // Output is INT, except if both inputs are unsigned and the op is +.
      if (ltype == rtype && projection.getOperation() == Operation::PLUS) {
        out_column_types.push_back(ltype);
      } else {
        out_column_types.push_back(sqlast::ColumnDefinition::Type::INT);
      }
    }
  }

  this->output_schema_ =
      SchemaFactory::Create(out_column_names, out_column_types, out_keys);
}

// TODO(babman): In order to preserve the projected names, we may create a
//               project operator that is purely an aliasing project.
//               As optimization, we should introduce an O(1) schema swap
//               operation into dataflow::Record, and use it here when aliasing
//               instead of copying/reconstructing the record.
std::vector<Record> ProjectOperator::Process(NodeIndex source,
                                             std::vector<Record> &&records,
                                             const Promise &promise) {
  std::vector<Record> output;
  for (const Record &record : records) {
    output.emplace_back(this->output_schema_, record.IsPositive());
    Record &out_record = output.back();
    for (size_t i = 0; i < this->projections_.size(); i++) {
      const auto &projection = this->projections_.at(i);

      if (projection.column()) {
        // Project an existing column.
        ColumnID column = projection.getLeftColumn();
        out_record.SetPolicy(i, record.CopyPolicy(column));
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
          case sqlast::ColumnDefinition::Type::TEXT: {
            out_record.SetString(
                std::make_unique<std::string>(record.GetString(column)), i);
            break;
          }
          // TODO(malte): DATETIME should not be stored as a string,
          // see below
          case sqlast::ColumnDefinition::Type::DATETIME:
            out_record.SetDateTime(
                std::make_unique<std::string>(record.GetDateTime(column)), i);
            break;
          default:
            LOG(FATAL) << "Unsupported column type "
                       << this->output_schema_.TypeOf(i) << " in project";
        }

      } else if (projection.literal()) {
        // Project a literal.
        const sqlast::Value &literal = projection.getLeftLiteral();
        switch (this->output_schema_.TypeOf(i)) {
          case sqlast::ColumnDefinition::Type::UINT:
            out_record.SetUInt(literal.GetUInt(), i);
            break;
          case sqlast::ColumnDefinition::Type::INT:
            out_record.SetInt(literal.GetInt(), i);
            break;
          case sqlast::ColumnDefinition::Type::TEXT:
            out_record.SetString(
                std::make_unique<std::string>(literal.GetString()), i);
            break;
          default:
            LOG(FATAL) << "Unsupported literal type in project";
        }

      } else {
        if (projection.left_column() && projection.right_column()) {
          ColumnID left = projection.getLeftColumn();
          ColumnID right = projection.getRightColumn();

          // Combine policies.
          const auto &left_policy = record.GetPolicy(left);
          const auto &right_policy = record.GetPolicy(right);
          if (left_policy != nullptr && right_policy != nullptr) {
            out_record.SetPolicy(i, left_policy->Combine(right_policy));
          } else if (left_policy != nullptr) {
            out_record.SetPolicy(i, left_policy->Copy());
          } else if (right_policy != nullptr) {
            out_record.SetPolicy(i, right_policy->Copy());
          }

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
          const sqlast::Value &right = projection.getRightLiteral();
          out_record.SetPolicy(i, record.CopyPolicy(left));
          switch (projection.getOperation()) {
            case Operation::MINUS: {
              ARITHMETIC_WITH_RIGHT_LITERAL_MACRO(-)
              break;
            }
            case Operation::PLUS: {
              ARITHMETIC_WITH_RIGHT_LITERAL_MACRO(+)
              break;
            }
            default:
              LOG(FATAL) << "Unsupported arithmetic operation in project";
          }

        } else {
          const sqlast::Value &left = projection.getLeftLiteral();
          ColumnID right = projection.getRightColumn();
          out_record.SetPolicy(i, record.CopyPolicy(right));
          switch (projection.getOperation()) {
            case Operation::MINUS: {
              ARITHMETIC_WITH_LEFT_LITERAL_MACRO(-)
              break;
            }
            case Operation::PLUS: {
              ARITHMETIC_WITH_LEFT_LITERAL_MACRO(+)
              break;
            }
            default:
              LOG(FATAL) << "Unsupported arithmetic operation in project";
          }
        }
      }
    }
  }

  return output;
}

std::unique_ptr<Operator> ProjectOperator::Clone() const {
  auto clone = std::make_unique<ProjectOperator>();
  clone->projections_ = this->projections_;
  return clone;
}

}  // namespace dataflow
}  // namespace k9db
