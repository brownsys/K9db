#ifndef PELTON_DATAFLOW_OPS_PROJECT_ENUM_H_
#define PELTON_DATAFLOW_OPS_PROJECT_ENUM_H_

namespace pelton {
namespace dataflow {

enum ProjectOperationEnum {
  MINUS,
  PLUS,
  NONE,
};

enum ProjectMetadataEnum {
  // A simple projection on a column
  COLUMN,
  // Projection does not involve column. Eg. SELECT 1 AS one FROM table_name;
  LITERAL,
  // Arithmetic operation with a literal and a column
  ARITHMETIC_WITH_LITERAL_LEFT,   // Literal on the left.
  ARITHMETIC_WITH_LITERAL_RIGHT,  // Literal on the right.
  // Arithmetic operation with right operand as another column.
  ARITHMETIC_WITH_COLUMN
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_PROJECT_ENUM_H_
