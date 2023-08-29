// Create table statement and sub expressions.
#ifndef K9DB_SQLAST_AST_SCHEMA_ENUMS_H_
#define K9DB_SQLAST_AST_SCHEMA_ENUMS_H_

namespace k9db {
namespace sqlast {

enum ColumnConstraintTypeEnum {
  PRIMARY_KEY,
  UNIQUE,
  FOREIGN_KEY,
  NOT_NULL,
  AUTO_INCREMENT,
  DEFAULT,
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
}  // namespace k9db

#endif  // K9DB_SQLAST_AST_SCHEMA_ENUMS_H_
