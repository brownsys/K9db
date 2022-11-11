// Parser responsible for parsing a simple  and limited set of sql statements
// which we want to parse quickly without ANTLR.
#include "pelton/sqlast/hacky.h"

#include <utility>

#include "pelton/sqlast/hacky_util.h"
#include "pelton/util/status.h"

namespace pelton {
namespace sqlast {

// Helper for parsing inserts and replaces.
absl::StatusOr<InsertOrReplace> HackyInsertOrReplace(const char *str,
                                                     size_t size) {
  // (INSERT|REPLACE) INTO <tablename> VALUES(<val>, <val>, <val>, ...)
  InsertOrReplace components;

  // (INSERT|REPLACE).
  const char *tmp_str = str;
  size_t tmp_size = size;
  if (StartsWith(&tmp_str, &tmp_size, "INSERT", 6)) {
    components.replace = false;
    str = tmp_str;
    size = tmp_size;
  } else if (StartsWith(&str, &size, "REPLACE", 7)) {
    components.replace = true;
  } else {
    return absl::InvalidArgumentError("Hacky insert: INSERT | REPLACE");
  }
  ConsumeWhiteSpace(&str, &size);

  // INTO.
  if (!StartsWith(&str, &size, "INTO", 4)) {
    return absl::InvalidArgumentError("Hacky insert: INTO");
  }
  ConsumeWhiteSpace(&str, &size);

  // <tablename>.
  components.table_name = ExtractIdentifier(&str, &size);
  if (components.table_name.size() == 0) {
    return absl::InvalidArgumentError("Hacky insert: table name");
  }
  ConsumeWhiteSpace(&str, &size);

  // Has columns inlined.
  if (StartsWith(&str, &size, "(", 1)) {
    ConsumeWhiteSpace(&str, &size);
    while (true) {
      components.columns.push_back(ExtractIdentifier(&str, &size));
      ConsumeWhiteSpace(&str, &size);
      if (!StartsWith(&str, &size, ",", 1)) {
        break;
      }
      ConsumeWhiteSpace(&str, &size);
    }
    if (!StartsWith(&str, &size, ")", 1)) {
      return absl::InvalidArgumentError("Hacking insert: columns )");
    }
    ConsumeWhiteSpace(&str, &size);
  }

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
    components.values.push_back(ExtractValue(&str, &size));
    ConsumeWhiteSpace(&str, &size);
  } while (StartsWith(&str, &size, ",", 1));

  // )
  if (!StartsWith(&str, &size, ")", 1)) {
    return absl::InvalidArgumentError("Hacky insert: )");
  }
  ConsumeWhiteSpace(&str, &size);

  // ;
  if (size != 0 && !StartsWith(&str, &size, ";", 1)) {
    return absl::InvalidArgumentError("Hacky insert: ;");
  }
  return components;
}

absl::StatusOr<std::unique_ptr<AbstractStatement>> HackyInsert(const char *str,
                                                               size_t size) {
  ASSIGN_OR_RETURN(InsertOrReplace & components,
                   HackyInsertOrReplace(str, size));
  if (components.replace) {
    return absl::InvalidArgumentError("REPLACE found instead of INSERT");
  }

  // Create the insert statement.
  std::unique_ptr<Insert> stmt =
      std::make_unique<Insert>(components.table_name);

  // Add columns.
  stmt->SetColumns(std::move(components.columns));

  // Parse and add values.
  std::vector<Value> parsed_values;
  parsed_values.reserve(components.values.size());
  for (const std::string &str : components.values) {
    parsed_values.push_back(Value::FromSQLString(str));
  }
  stmt->SetValues(std::move(parsed_values));
  return stmt;
}

absl::StatusOr<std::unique_ptr<AbstractStatement>> HackySelect(const char *str,
                                                               size_t size) {
  // SELECT <cols>, ... FROM <tablename> WHERE <colum_name> = <value>
  // SELECT.
  if (!StartsWith(&str, &size, "SELECT", 6)) {
    return absl::InvalidArgumentError("Hacky select: SELECT");
  }
  ConsumeWhiteSpace(&str, &size);

  // <col>, ...
  std::vector<Select::ResultColumn> columns;
  while (true) {
    std::string unparsed = ExtractIdentifier(&str, &size);
    if (unparsed.size() == 0) {
      return absl::InvalidArgumentError("Hacky select: COLUMN");
    }

    char c = unparsed[0];
    if (c == '"' || c == '\'' || std::isdigit(c) || c == '-') {  // Literal.
      columns.emplace_back(Value::FromSQLString(unparsed));
    } else {  // Column name  (or *).
      columns.emplace_back(unparsed);
    }

    ConsumeWhiteSpace(&str, &size);
    if (!StartsWith(&str, &size, ",", 1)) {
      break;
    }
    ConsumeWhiteSpace(&str, &size);
  }
  ConsumeWhiteSpace(&str, &size);

  // FROM.
  if (!StartsWith(&str, &size, "FROM", 4)) {
    return absl::InvalidArgumentError("Hacky select: FROM");
  }
  ConsumeWhiteSpace(&str, &size);

  // <tablename>.
  std::string table_name = ExtractIdentifier(&str, &size);
  if (table_name.size() == 0) {
    return absl::InvalidArgumentError("Hacky select: table name");
  }
  ConsumeWhiteSpace(&str, &size);

  // Create the select statement.
  std::unique_ptr<Select> stmt = std::make_unique<Select>(table_name);
  for (const Select::ResultColumn &col : columns) {
    stmt->AddColumn(col);
  }

  // WHERE.
  if (StartsWith(&str, &size, "WHERE", 5)) {
    ConsumeWhiteSpace(&str, &size);

    // Condition.
    auto condition = HackyCondition(&str, &size);
    if (condition == nullptr) {
      return absl::InvalidArgumentError("Hacky select: condition");
    }
    stmt->SetWhereClause(std::move(condition));
  }

  // End of statement.
  if (size != 0 && !StartsWith(&str, &size, ";", 1)) {
    return absl::InvalidArgumentError("Hacky select: ;");
  }

  return stmt;
}

absl::StatusOr<std::unique_ptr<AbstractStatement>> HackyGDPR(const char *str,
                                                             size_t size) {
  // GDPR (FORGET | GET) shard_kind user_id;
  // GDPR.
  if (!StartsWith(&str, &size, "GDPR", 4)) {
    return absl::InvalidArgumentError("Hacky GDPR: GDPR");
  }
  ConsumeWhiteSpace(&str, &size);

  // GET or FORGET.
  std::string operation_str = ExtractIdentifier(&str, &size);
  GDPRStatement::Operation operation;
  if (EqualIgnoreCase(operation_str, "GET")) {
    operation = GDPRStatement::Operation::GET;
  } else if (EqualIgnoreCase(operation_str, "FORGET")) {
    operation = GDPRStatement::Operation::FORGET;
  } else {
    return absl::InvalidArgumentError("Hacky GDPR: operation name");
  }
  ConsumeWhiteSpace(&str, &size);

  // <shard_kind>.
  std::string shard_kind = ExtractIdentifier(&str, &size);
  if (shard_kind.size() == 0) {
    return absl::InvalidArgumentError("Hacky GDPR: shard kind");
  }
  ConsumeWhiteSpace(&str, &size);

  // <user_id>.
  Value user_id = Value::FromSQLString(ExtractValue(&str, &size));
  ConsumeWhiteSpace(&str, &size);

  return std::make_unique<GDPRStatement>(operation, shard_kind, user_id);
}

absl::StatusOr<std::unique_ptr<AbstractStatement>> HackyParse(
    const std::string &sql) {
  size_t size = sql.size();
  const char *str = sql.data();

  if (str[0] == 'I' || str[0] == 'i' || str[0] == 'R' || str[0] == 'r') {
    return HackyInsert(str, size);
  }
  if (str[0] == 'S' || str[0] == 's') {
    return HackySelect(str, size);
  }
  if (str[0] == 'G' || str[0] == 'g') {
    return HackyGDPR(str, size);
  }

  return absl::InvalidArgumentError("Cannot hacky parse");
}

}  // namespace sqlast
}  // namespace pelton
