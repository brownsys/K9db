#include "k9db/sqlast/ast_policy.h"

// NOLINTNEXTLINE
#include <regex>

#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "glog/logging.h"

namespace k9db {
namespace sqlast {

// Regexes for parsing parameters.
namespace {

// ${self.<column_name>}.
#define COLUMN_EXPR "\\$\\{self\\.([A-Za-z0-9_]+)\\}"

// Regexes made out of above components.
static std::regex REGEX{COLUMN_EXPR};

std::vector<std::string> GetAllColumns(std::string query) {
  std::vector<std::string> columns;
  for (std::smatch m; std::regex_search(query, m, REGEX); query = m.suffix()) {
    std::string column = m.str();
    columns.push_back(column.substr(7, column.size() - 7 - 1));
  }
  return columns;
}

std::string ParameterizeQurey(const std::string &query) {
  return std::regex_replace(query, REGEX, "?");
}

PolicySchema::SinglePolicy ParseSinglePolicy(const std::string &schema) {
  std::vector<std::string> split = absl::StrSplit(schema, ";");
  CHECK_GE(split.size(), 1u) << "Not enough arguments to policy";

  // Parse components.
  std::string &policy_name = split.at(0);
  std::vector<PolicyExpression> expressions;
  for (size_t i = 1; i < split.size(); i++) {
    expressions.push_back(PolicyExpression::Parse(std::move(split.at(i))));
  }

  return PolicySchema::SinglePolicy{std::move(policy_name),
                                    std::move(expressions)};
}

// For debugging.
std::string HelperDescribe(const PolicySchema::SinglePolicy &policy) {
  std::vector<std::string> strings;
  for (const auto &expression : policy.expressions) {
    strings.push_back(expression.Describe());
  }
  return policy.name + "(" + absl::StrJoin(strings, ", ") + ")";
}

}  // namespace

// PolicyQuery
PolicyQuery PolicyQuery::Parse(const std::string &query) {
  // Parse query looking for any ${self.<column>} expressions.
  std::vector<std::string> columns = GetAllColumns(query);
  std::string parameterized_query = ParameterizeQurey(query);

  return PolicyQuery(std::move(parameterized_query), std::move(columns));
}

std::string PolicyQuery::Describe() const {
  if (this->params_.size() == 0) {
    return this->query_;
  }
  return this->query_ + ", " + absl::StrJoin(this->params_, ",");
}

// PolicyExpression
PolicyExpression PolicyExpression::Parse(const std::string &expr) {
  // All expressions are on the form <symbol>{<expr>}
  // Determine what kind of expression we are looking at.

  // Literal value!
  if (expr[0] == 'v') {
    std::string literal = expr.substr(2, expr.size() - 3);
    return PolicyExpression(Value::FromSQLString(literal));
  }

  // Column expression!
  if (expr[0] == '$') {
    std::vector<std::string> columns = GetAllColumns(expr);
    CHECK_EQ(columns.size(), 1u) << "Illegal column policy expression";
    return PolicyExpression(std::move(columns.at(0)));
  }

  // Query.
  if (expr[0] == 'q') {
    std::string query = expr.substr(2, expr.size() - 3);
    return PolicyExpression(PolicyQuery::Parse(query));
  }

  LOG(FATAL) << "Unrecorgnized policy expression '" << expr << "'";
}
std::string PolicyExpression::Describe() const {
  if (this->expression_.index() == 0) {
    return "v{" + std::get<0>(this->expression_).AsSQLString() + "}";
  }
  if (this->expression_.index() == 1) {
    return "${" + std::get<1>(this->expression_) + "}";
  }
  if (this->expression_.index() == 2) {
    return "q{" + std::get<2>(this->expression_).Describe() + "}";
  }
  LOG(FATAL) << "Bad variant";
}

// PolicySchema
PolicySchema PolicySchema::Parse(const std::string &schema) {
  if (schema[0] == 'P') {
    std::string parsed = schema.substr(2, schema.size());
    SinglePolicy policy = ParseSinglePolicy(parsed);
    return PolicySchema(Type::SINGLE, {policy});
  }

  if (schema[0] == '&' || schema[0] == '|') {
    Type op = schema[0] == '&' ? Type::AND : Type::OR;
    std::vector<SinglePolicy> policies;

    std::string parsed = schema.substr(2, schema.size());
    std::vector<std::string> split = absl::StrSplit(parsed, " ~ ");
    for (size_t i = 0; i < split.size(); i++) {
      policies.push_back(ParseSinglePolicy(split.at(i)));
    }

    return PolicySchema(op, std::move(policies));
  }

  LOG(FATAL) << "Unrecorgnized policy schema '" << schema << "'";
}
std::string PolicySchema::Describe() const {
  std::string delim = this->type_ == Type::AND ? " & " : " | ";
  std::vector<std::string> strings;
  for (const auto &policy : this->policies_) {
    strings.push_back(HelperDescribe(policy));
  }
  return absl::StrJoin(strings, delim);
}

// PolicyStatement
PolicyStatement PolicyStatement::Parse(const std::string &statement) {
  // POLICY <table>.<column> <policy schema>
  CHECK(statement.starts_with("POLICY "));
  CHECK_GT(statement.size(), 7u);

  // Parse table name.
  size_t start = 7;
  size_t end = 7;
  while (statement[end] != '.') {
    CHECK_LT(++end, statement.size()) << "No table.column!";
  }
  std::string table_name = statement.substr(start, end - start);

  // Parse column name.
  start = ++end;
  CHECK_LT(end, statement.size());
  while (statement[end] != ' ') {
    CHECK_LT(++end, statement.size()) << "No table.column!";
  }
  std::string column_name = statement.substr(start, end - start);

  // Get remaining policy schema.
  CHECK_LT(end + 1, statement.size()) << "No schema!";
  std::string schema = statement.substr(end + 1, statement.size());
  PolicySchema parsed = PolicySchema::Parse(schema);

  // Done!
  return PolicyStatement(std::move(table_name), std::move(column_name),
                         std::move(parsed));
}

}  // namespace sqlast
}  // namespace k9db
