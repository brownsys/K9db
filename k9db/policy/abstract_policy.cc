#include "k9db/policy/abstract_policy.h"

namespace k9db {
namespace policy {

bool AbstractPolicy::Check() const { return true; }

std::unique_ptr<AbstractPolicy> AbstractPolicy::Combine(AbstractPolicy *other) {
  return nullptr;
}

bool AbstractPolicy::Allows(dataflow::OperatorType type) const { return true; }

}  // namespace policy
}  // namespace k9db
