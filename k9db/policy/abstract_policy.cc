#include "k9db/policy/abstract_policy.h"

#include <iostream>

namespace k9db {
namespace policy {

std::unique_ptr<AbstractPolicy> AbstractPolicy::Combine(
    const std::unique_ptr<AbstractPolicy> &other) const {
  auto o = other == nullptr ? "[nullptr]" : other->Debug();
  std::cout << "Combing " << this->Debug() << "\n with " << o << std::endl
            << std::endl;
  return nullptr;
}
std::unique_ptr<AbstractPolicy> AbstractPolicy::Subtract(
    const std::unique_ptr<AbstractPolicy> &other) const {
  auto o = other == nullptr ? "[nullptr]" : other->Debug();
  std::cout << "Subtracting " << this->Debug() << "\n with " << o << std::endl
            << std::endl;
  return nullptr;
}

bool AbstractPolicy::Allows(dataflow::OperatorType type) const { return true; }

}  // namespace policy
}  // namespace k9db
