#ifndef K9DB_POLICY_POLICY_STATE_H_
#define K9DB_POLICY_POLICY_STATE_H_

#include <string>
#include <unordered_map>
#include <utility>

#include "k9db/policy/policy_builder.h"
#include "k9db/sqlast/ast_policy.h"
#include "k9db/sqlast/ast_value.h"

namespace k9db {
namespace policy {

// Policy state (per table).
class PolicyState {
 public:
  PolicyState() : map_() {}

  void AddPolicy(const std::string &column_name, PolicyBuilder &&builder) {
    this->map_.emplace(column_name, std::move(builder));
  }
  bool HasPolicy(const std::string &column_name) const {
    return this->map_.contains(column_name);
  }
  const PolicyBuilder &GetPolicyBuilder(const std::string &column_name) const {
    return this->map_.at(column_name);
  }

  // const-accessor.
  const std::unordered_map<std::string, PolicyBuilder> &map() const {
    return this->map_;
  }

 private:
  // Column Name -> Policy Builder.
  std::unordered_map<std::string, PolicyBuilder> map_;
};

}  // namespace policy
}  // namespace k9db

#endif  // K9DB_POLICY_POLICY_STATE_H_
