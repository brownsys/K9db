#ifndef K9DB_SQLAST_AST_POLICY_H_
#define K9DB_SQLAST_AST_POLICY_H_

#include <string>
#include <utility>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "k9db/sqlast/ast_value.h"

namespace k9db {
namespace sqlast {

// A literal value.
using PolicyLiteral = Value;

// A column in the table this policy is applied to.
using PolicyColumn = std::string;

// A sub-query; possibly with parameters.
class PolicyQuery {
 public:
  static PolicyQuery Parse(const std::string &table_name,
                           const std::string &column_name,
                           const std::string &policy_name, size_t index,
                           const std::string &query);

  // Constructor.
  PolicyQuery(std::string &&query, std::string &&flow_name,
              std::vector<PolicyColumn> &&params)
      : query_(std::move(query)),
        flow_name_(std::move(flow_name)),
        params_(std::move(params)) {}

  // For debugging.
  std::string ToString() const;

 public:
  std::string query_;
  std::string flow_name_;
  std::vector<PolicyColumn> params_;

  // Construct a unique flow name for this query.
  static std::string CreateFlowName(const std::string &table,
                                    const std::string &column,
                                    const std::string &policy_name,
                                    size_t index) {
    std::string index_str = std::to_string(index);
    return "__P_" + table + "_" + column + "_" + policy_name + "_" + index_str;
  }
};

// An expression describing an element of the policy.
class PolicyExpression {
 public:
  static PolicyExpression Parse(const std::string &table_name,
                                const std::string &column_name,
                                const std::string &policy_name, size_t index,
                                const std::string &expr);

  // Constructor.
  explicit PolicyExpression(PolicyLiteral &&literal)
      : expression_(std::move(literal)) {}

  explicit PolicyExpression(PolicyColumn &&column)
      : expression_(std::move(column)) {}

  explicit PolicyExpression(PolicyQuery &&query)
      : expression_(std::move(query)) {}

  // For debugging.
  std::string ToString() const;

  /*
  // Evaluate the expression given a record from a base table.
  std::vector<Value> Evaluate(
      const dataflow::DataFlowState &state,
      const dataflow::Record &record) const;
  */

 public:
  std::variant<PolicyLiteral, PolicyColumn, PolicyQuery> expression_;
};

// Define the schema of a policy associated to some column in some table.
class PolicySchema {
 public:
  // The content of a single policy schema.
  struct SinglePolicy {
    std::string name;
    std::vector<PolicyExpression> expressions;
  };

  // Policy schema may contain a single policy schema or an AND or OR of
  // sub-schemas.
  enum class Type { SINGLE, AND, OR };

  // Parse schema from string represetation.
  static PolicySchema Parse(const std::string &table_name,
                            const std::string &column_name,
                            const std::string &schema);

  // Constructor.
  PolicySchema(Type type, std::vector<SinglePolicy> &&policies)
      : type_(type), policies_(std::move(policies)) {}

  // For debugging.
  std::string ToString() const;

  /*
  // Create a policy of this given schema for the given row.
  void CreatePolicyForRow(const dataflow::DataFlowState &dstate,
                          const std::string &table, const std::string &column,
                          const dataflow::Record &record) const;
  */

 private:
  Type type_;
  std::vector<SinglePolicy> policies_;
};

// POLICY <table>.<column> <policy schema>
class PolicyStatement {
 public:
  // Parse the policy statement.
  static PolicyStatement Parse(const std::string &statement);

  // Constructor.
  PolicyStatement(std::string &&table, std::string &&column,
                  PolicySchema &&policy_schema)
      : table_(std::move(table)),
        column_(std::move(column)),
        policy_schema_(std::move(policy_schema)) {}

  /*
  // Create a policy of this given schema for the given row.
  void CreatePolicyForRow(const dataflow::DataFlowState &dstate,
                          const dataflow::Record &record) const;
  */

  // Accessors.
  const std::string &table() const { return this->table_; }
  const std::string &column() const { return this->column_; }
  const PolicySchema &schema() const { return this->policy_schema_; }

 private:
  std::string table_;
  std::string column_;
  PolicySchema policy_schema_;
};

}  // namespace sqlast
}  // namespace k9db

#endif  // K9DB_SQLAST_AST_POLICY_H_
