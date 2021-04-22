// Parser responsible for parsing a simple  and limited set of sql statements
// which we want to parse quickly without ANTLR.
#include "pelton/sqlast/hacky.h"

#include "pelton/sqlast/hacky_util.h"

namespace pelton {
namespace sqlast {

absl::StatusOr<std::unique_ptr<AbstractStatement>> HackyInsert(const char *str,
                                                               size_t size) {
  // INSERT INTO <tablename> VALUES(<val>, <val>, <val>, ...)
  // INSERT.
  if (!StartsWith(&str, &size, "INSERT", 6)) {
    return absl::InvalidArgumentError("Hacky insert: INSERT");
  }
  ConsumeWhiteSpace(&str, &size);

  // INTO.
  if (!StartsWith(&str, &size, "INTO", 4)) {
    return absl::InvalidArgumentError("Hacky insert: INTO");
  }
  ConsumeWhiteSpace(&str, &size);

  // <tablename>.
  std::string table_name = ExtractIdentifier(&str, &size);
  if (table_name.size() == 0) {
    return absl::InvalidArgumentError("Hacky insert: table name");
  }
  ConsumeWhiteSpace(&str, &size);
  
  // Create the insert statement.
  std::unique_ptr<Insert> stmt = std::make_unique<Insert>(table_name);

  // VALUES.
  if (!StartsWith(&str, &size, "VALUES", 6)) {
    return absl::InvalidArgumentError("Hacky insert: VALUES");
  }
  ConsumeWhiteSpace(&str, &size);

  // (
  if (!StartsWith(&str, &size, "(", 1)) {
    return absl::InvalidArgumentError("Hacky insert: (");
  }
  ConsumeWhiteSpace(&str, &size);
  
  do {
    stmt->AddValue(ExtractValue(&str, &size));
    ConsumeWhiteSpace(&str, &size);
  } while (StartsWith(&str, &size, ",", 1));

  // )
  if (!StartsWith(&str, &size, ");", 2)) {
    return absl::InvalidArgumentError("Hacky insert: );");
  }
  ConsumeWhiteSpace(&str, &size);
  
  if (size != 0) {
    return absl::InvalidArgumentError("Hacky insert: EOF");
  }

  return stmt;
}



absl::StatusOr<std::unique_ptr<AbstractStatement>> HackyParse(
    const std::string &sql) {
  size_t size = sql.size();
  const char *str = sql.c_str();

  if (str[0] == 'I' || str[0] == 'i') {
    return HackyInsert(str, size);
  }

  return absl::InvalidArgumentError("Cannot hacky parse");
}

}  // namespace sqlast
}  // namespace pelton
