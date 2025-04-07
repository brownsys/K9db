#include "k9db/policy/policy_builder.h"

#include <vector>

#include "glog/logging.h"
#include "k9db/dataflow/key.h"
#include "k9db/policy/policies/policies.h"
#include "k9db/shards/sqlengine/view.h"
#include "k9db/sql/result.h"
#include "k9db/sqlast/ast_value.h"
#include "k9db/util/status.h"

namespace k9db {
namespace policy {

// Static/Global map of query expression to flow names.
std::unordered_map<std::string, std::string> PolicyBuilder::FLOWS =
    std::unordered_map<std::string, std::string>();

// Make any required flows when builder is created.
void PolicyBuilder::MakeFlows(Connection *connection, util::UniqueLock *lock) {
  for (auto &policy : this->policy_schema_.policies()) {
    for (auto &expression : policy.expressions) {
      sqlast::PolicyExpression::Variant &expr = expression.expression();
      if (expr.index() == 2) {
        sqlast::PolicyQuery &query = std::get<2>(expr);
        const std::string &query_stmt = query.query();

        // Re-use view or create a new one.
        if (PolicyBuilder::FLOWS.contains(query_stmt)) {
          query.flow_name() = PolicyBuilder::FLOWS.at(query_stmt);
        } else {
          std::string flow_name =
              "$^P_" + std::to_string(PolicyBuilder::FLOWS.size());

          // Store flow name.
          query.flow_name() = flow_name;
          PolicyBuilder::FLOWS.emplace(query_stmt, flow_name);

          // Create the data flow.
          sqlast::CreateView create_view(flow_name, query_stmt);
          MOVE_OR_PANIC(sql::SqlResult result,
                        shards::sqlengine::view::CreateView(create_view,
                                                            connection, lock));
          CHECK(result.Success()) << "Cannot create view with " << query_stmt;
        }
      }
    }
  }
}

// Policy instance creation for DB records.
std::unique_ptr<AbstractPolicy> PolicyBuilder::MakePolicy(
    const dataflow::Record &record,
    const dataflow::DataFlowState &dstate) const {
  // Make an instance of each policy component.
  std::vector<std::unique_ptr<AbstractPolicy>> policies;
  for (const auto &policy : this->policy_schema_.policies()) {
    std::vector<sqlast::Value> values;
    for (const auto &expr : policy.expressions) {
      const sqlast::PolicyExpression::Variant &variant = expr.expression();
      if (variant.index() == 0) {
        values.push_back(std::get<0>(variant));
      } else if (variant.index() == 1) {
        const std::string &column = std::get<1>(variant);
        values.push_back(record.GetValue(record.schema().IndexOf(column)));
      } else if (variant.index() == 2) {
        // Nested query; must query view.
        const sqlast::PolicyQuery &query = std::get<2>(variant);
        const std::string &flow_name = query.flow_name();
        const std::vector<std::string> &params = query.params();

        // Build key to query view.
        dataflow::Key key(params.size());
        for (const std::string &column : params) {
          key.AddValue(record.GetValue(record.schema().IndexOf(column)));
        }

        // Now can query view.
        std::vector<dataflow::Record> result =
            dstate.GetFlow(flow_name).Lookup(key, -1, 0);
        for (const dataflow::Record &r : result) {
          values.push_back(r.GetValue(0));
        }
      } else {
        LOG(FATAL) << "UNREACHABLE VARIANT!";
      }
    }

    policies.push_back(policies::MakePolicy(policy.name, std::move(values)));
  }

  // Combine policies according to type.
  sqlast::PolicySchema::Type type = this->policy_schema_.type();
  if (type == sqlast::PolicySchema::Type::SINGLE) {
    CHECK_EQ(policies.size(), 1u) << "INCONSISTENT POLICY SCHEMA";
    return std::move(policies.at(0));
  } else if (type == sqlast::PolicySchema::Type::AND) {
    CHECK_GT(policies.size(), 1u) << "FEW AND POLICIES";
    return std::make_unique<policies::And>(std::move(policies));
  } else if (type == sqlast::PolicySchema::Type::OR) {
    CHECK_GT(policies.size(), 1u) << "FEW OR POLICIES";
    return std::make_unique<policies::Or>(std::move(policies));
  } else {
    LOG(FATAL) << "UNREACHABLE!";
  }
}

}  // namespace policy
}  // namespace k9db
