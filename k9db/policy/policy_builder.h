#ifndef K9DB_POLICY_POLICY_BUILDER_H_
#define K9DB_POLICY_POLICY_BUILDER_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "k9db/dataflow/record.h"
#include "k9db/dataflow/state.h"
#include "k9db/policy/abstract_policy.h"
#include "k9db/sqlast/ast_policy.h"
#include "k9db/util/upgradable_lock.h"

namespace k9db {

// Forward declaration.
class Connection;

namespace policy {

// Policy state (per table).
class PolicyBuilder {
 public:
  explicit PolicyBuilder(const sqlast::PolicySchema &schema)
      : policy_schema_(schema) {}

  // Create flows (builder creation time).
  void MakeFlows(Connection *connection, util::UniqueLock *lock);

  // Make a policy object for the given DB row.
  std::unique_ptr<AbstractPolicy> MakePolicy(
      const dataflow::Record &record,
      const dataflow::DataFlowState &dstate) const;

  // Describes the policy schema for SHOW POLICIES.
  std::string Describe() const { return this->policy_schema_.Describe(); }

 private:
  sqlast::PolicySchema policy_schema_;

  // OK to use global static here: k9db::State is a singleton + this is only
  // accessed with unique lock on state.
  static std::unordered_map<std::string, std::string> FLOWS;
};

}  // namespace policy
}  // namespace k9db

#endif  // K9DB_POLICY_POLICY_BUILDER_H_
