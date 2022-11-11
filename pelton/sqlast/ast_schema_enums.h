// Create table statement and sub expressions.
#ifndef PELTON_SQLAST_AST_SCHEMA_ENUMS_H_
#define PELTON_SQLAST_AST_SCHEMA_ENUMS_H_

namespace pelton {
namespace sqlast {

enum ColumnConstraintTypeEnum {
  PRIMARY_KEY,
  UNIQUE,
  FOREIGN_KEY,
};
enum ForeignKeyTypeEnum {
  OWNED_BY,
  OWNS,
  ACCESSED_BY,
  ACCESSES,
  AUTO,  // Nothing is explicitly stated. Enabled implicit deduction.
  PLAIN  // Explicitly disables deduction. No ownership.
};
enum ColumnDefinitionTypeEnum { UINT = 0, INT = 1, TEXT = 2, DATETIME = 3 };
enum AnonymizationOpTypeEnum { GET = 0, DEL = 1 };

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_AST_SCHEMA_ENUMS_H_
