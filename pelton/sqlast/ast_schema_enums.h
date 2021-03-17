// Create table statement and sub expressions.
#ifndef PELTON_SQLAST_AST_SCHEMA_ENUMS_H_
#define PELTON_SQLAST_AST_SCHEMA_ENUMS_H_

namespace pelton {
namespace sqlast {

enum ColumnConstraintTypeEnum { PRIMARY_KEY, UNIQUE, NOT_NULL, FOREIGN_KEY };
enum ColumnDefinitionTypeEnum { UINT, INT, TEXT, DATETIME };

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_AST_SCHEMA_ENUMS_H_
