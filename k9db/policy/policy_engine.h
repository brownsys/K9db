#ifndef K9DB_POLICY_POLICY_ENGINE_H_
#define K9DB_POLICY_POLICY_ENGINE_H_

#include <string>

#include "k9db/connection.h"

namespace k9db {
namespace policy {

void AddPolicy(const std::string &stmt, Connection *connection);


}  // namespace policy
}  // namespace k9db

#endif  // K9DB_POLICY_POLICY_ENGINE_H_
