// This file contains utility functions, mostly for standarizing
// name (suffixes etc) of things.
#include "pelton/shards/sqlengine/util.h"

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"

namespace pelton {
namespace shards {
namespace sqlengine {

bool IsADataSubject(const sqlast::CreateTable &stmt) {
  for (const sqlast::ColumnDefinition &column : stmt.GetColumns()) {
    if (absl::StartsWith(column.column_name(), "PII_")) {
      return true;
    }
  }
  return false;
}

std::string Dequote(const std::string &st) {
  std::string s(st);
  s.erase(std::remove(s.begin(), s.end(), '\"'), s.end());
  s.erase(std::remove(s.begin(), s.end(), '\''), s.end());
  return s;
}

bool IsOwner(const sqlast::ColumnDefinition &col) {
  return absl::StartsWith(col.column_name(), "OWNER_");
}

bool IsAccessor(const sqlast::ColumnDefinition &col) {
  return absl::StartsWith(col.column_name(), "ACCESSOR_");
}

bool IsOwns(const sqlast::ColumnDefinition &col) {
  return absl::StartsWith(col.column_name(), "OWNING_");
}

bool IsAccesses(const sqlast::ColumnDefinition &col) {
  return absl::StartsWith(col.column_name(), "ACCESSING_");
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
