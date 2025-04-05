#ifndef K9DB_POLICY_POLICY_BUILDER_H_
#define K9DB_POLICY_POLICY_BUILDER_H_

#include <utility>

#include "k9db/sqlast/ast_policy.h"

namespace k9db {
namespace policy {

// Policy state (per table).
class PolicyBuilder {
 public:
  explicit PolicyBuilder(const sqlast::PolicySchema &schema)
      : policy_schema_(schema) {}

  const sqlast::PolicySchema &schema() const { return this->policy_schema_; }

 private:
  sqlast::PolicySchema policy_schema_;
};

}  // namespace policy
}  // namespace k9db

#endif  // K9DB_POLICY_POLICY_BUILDER_H_
