// Parser responsible for parsing a simple  and limited set of sql statements
// which we want to parse quickly without ANTLR.

#ifndef PELTON_SQLAST_HACKY_H_
#define PELTON_SQLAST_HACKY_H_

#include <memory>
#include <string>

#include "absl/status/statusor.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace sqlast {

absl::StatusOr<std::unique_ptr<AbstractStatement>> HackyInsert(const char *str,
                                                               size_t size);

absl::StatusOr<std::unique_ptr<AbstractStatement>> HackyParse(
    const std::string &sql);

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_HACKY_H_