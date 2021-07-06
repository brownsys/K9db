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

absl::StatusOr<std::unique_ptr<AbstractStatement>> HackySelect(const char *str,
                                                               size_t size) {
  // SELECT * FROM <tablename> WHERE <colum_name> = <value>
  // SELECT.
  if (!StartsWith(&str, &size, "SELECT", 6)) {
    return absl::InvalidArgumentError("Hacky select: SELECT");
  }
  ConsumeWhiteSpace(&str, &size);

  // * FROM.
  if (!StartsWith(&str, &size, "*", 1)) {
    return absl::InvalidArgumentError("Hacky select: *");
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
  stmt->AddColumn("*");

  // WHERE.
  if (!StartsWith(&str, &size, "WHERE", 5)) {
    return absl::InvalidArgumentError("Hacky select: WHERE");
  }
  ConsumeWhiteSpace(&str, &size);

  // <column_name>
  std::string column_name = ExtractIdentifier(&str, &size);
  if (column_name.size() == 0) {
    return absl::InvalidArgumentError("Hacky select: column name");
  }
  ConsumeWhiteSpace(&str, &size);

  // =.
  if (!StartsWith(&str, &size, "=", 1)) {
    return absl::InvalidArgumentError("Hacky select: =");
  }
  ConsumeWhiteSpace(&str, &size);

  // Value.
  std::string value = ExtractValue(&str, &size);
  if (value.size() == 0) {
    return absl::InvalidArgumentError("Hacky select: value");
  }
  ConsumeWhiteSpace(&str, &size);

  // End of statement.
  if (!StartsWith(&str, &size, ";", 1)) {
    return absl::InvalidArgumentError("Hacky select: ;");
  }
  if (size != 0) {
    return absl::InvalidArgumentError("Hacky select: EOF");
  }

  // Create condition and add it to the select stmt.
  std::unique_ptr<BinaryExpression> condition =
      std::make_unique<BinaryExpression>(Expression::Type::EQ);
  condition->SetLeft(std::make_unique<ColumnExpression>(column_name));
  condition->SetRight(std::make_unique<LiteralExpression>(value));
  stmt->SetWhereClause(std::move(condition));

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

  if (str[0] == 'I' || str[0] == 'i') {
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
