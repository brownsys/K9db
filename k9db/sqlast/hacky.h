// Parser responsible for parsing a simple  and limited set of sql statements
// which we want to parse quickly without ANTLR.

#ifndef K9DB_SQLAST_HACKY_H_
#define K9DB_SQLAST_HACKY_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "k9db/sqlast/ast.h"
#include "k9db/sqlast/command.h"

namespace k9db {
namespace sqlast {

struct InsertOrReplace {
  bool replace;  // true -> REPLACE, otherwise INSERT.
  std::string table_name;
  std::vector<std::string> columns;
  std::vector<std::string> values;
};
absl::StatusOr<InsertOrReplace> HackyInsertOrReplace(
    const char *str, size_t size, const std::vector<std::string> &args);

absl::StatusOr<std::unique_ptr<AbstractStatement>> HackyInsert(
    const char *str, size_t size, const std::vector<std::string> &args);

absl::StatusOr<std::unique_ptr<AbstractStatement>> HackySelect(
    const char *str, size_t size, const std::vector<std::string> &args);

absl::StatusOr<std::unique_ptr<AbstractStatement>> HackyUpdate(
    const char *str, size_t size, const std::vector<std::string> &args);

absl::StatusOr<std::unique_ptr<AbstractStatement>> HackyParse(
    const SQLCommand &sql);

}  // namespace sqlast
}  // namespace k9db

#endif  // K9DB_SQLAST_HACKY_H_
