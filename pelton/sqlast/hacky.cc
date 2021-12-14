// Parser responsible for parsing a simple  and limited set of sql statements
// which we want to parse quickly without ANTLR.
#include "pelton/sqlast/hacky.h"

#include <utility>

#include "pelton/sqlast/hacky_util.h"

namespace pelton {
namespace sqlast {                                                                 

absl::StatusOr<std::unique_ptr<AbstractStatement>> HackyInsert(const char *str,
                                                               size_t size) {
  // INSERT INTO <tablename> VALUES(<val>, <val>, <val>, ...)
  // INSERT.
  const char *replace_buf = str;
  size_t replace_size = size;
  bool insert = StartsWith(&str, &size, "INSERT", 6);
  bool replace = StartsWith(&replace_buf, &replace_size, "REPLACE", 7);
  if (!insert && !replace) {
    return absl::InvalidArgumentError("Hacky insert: INSERT | REPLACE");
  } else if (replace) {
    str = replace_buf;
    size = replace_size;
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
  std::unique_ptr<Insert> stmt = std::make_unique<Insert>(table_name, replace);

  // Has columns inlined.
  if (StartsWith(&str, &size, "(", 1)) {
    ConsumeWhiteSpace(&str, &size);
    while (true) {
      stmt->AddColumn(ExtractIdentifier(&str, &size));
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
    stmt->AddValue(ExtractValue(&str, &size));
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
  return stmt;
}

absl::StatusOr<std::unique_ptr<AbstractStatement>> HackySelect(const char *str,
                                                               size_t size) {
  // SELECT * FROM <tablename> WHERE <colum_name> = <value>
  // SELECT.
  if (!StartsWith(&str, &size, "SELECT", 6)) {
    return absl::InvalidArgumentError("Hacky select: SELECT");
  }
  ConsumeWhiteSpace(&str, &size);

  // * FROM.
  std::vector<std::string> columns;
  while (true) {
    columns.push_back(ExtractIdentifier(&str, &size));
    ConsumeWhiteSpace(&str, &size);
    if (!StartsWith(&str, &size, ",", 1)) {
      break;
    }
    ConsumeWhiteSpace(&str, &size);
  }
  ConsumeWhiteSpace(&str, &size);

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
  for (const std::string &col : columns) {
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
  std::string user_id = ExtractValue(&str, &size);
  if (user_id.size() == 0) {
    return absl::InvalidArgumentError("Hacky GDPR: user_id");
  }
  ConsumeWhiteSpace(&str, &size);

  return std::make_unique<GDPRStatement>(operation, shard_kind, user_id);
}

absl::StatusOr<std::unique_ptr<AbstractStatement>> HackyParse(
    const std::string &sql) {
  size_t size = sql.size();
  const char *str = sql.c_str();

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
