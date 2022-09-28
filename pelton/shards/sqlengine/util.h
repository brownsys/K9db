// This file contains utility functions, mostly for standarizing
// name (suffixes etc) of things.
//
// TODO(babman): All these functions should be gone and we should make them
// part of the AST by improving annotations and making AST typed.

#ifndef PELTON_SHARDS_SQLENGINE_UTIL_H_
#define PELTON_SHARDS_SQLENGINE_UTIL_H_

#include <string>

#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace sqlengine {

bool IsADataSubject(const sqlast::CreateTable &stmt);

std::string Dequote(const std::string &st);

bool IsOwner(const sqlast::ColumnDefinition &col);
bool IsAccessor(const sqlast::ColumnDefinition &col);
bool IsOwns(const sqlast::ColumnDefinition &col);
bool IsAccesses(const sqlast::ColumnDefinition &col);

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_UTIL_H_
