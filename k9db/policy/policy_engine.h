#ifndef K9DB_POLICY_POLICY_ENGINE_H_
#define K9DB_POLICY_POLICY_ENGINE_H_

#include <string>
#include <vector>

#include "k9db/connection.h"
#include "k9db/dataflow/record.h"

namespace k9db {
namespace policy {

void AddPolicy(const std::string &stmt, Connection *connection);

void MakePolicies(const std::string &table_name, Connection *connection,
                  std::vector<dataflow::Record> *records);

std::vector<dataflow::Record> SerializePolicies(
    const std::vector<dataflow::Record> &records);

}  // namespace policy
}  // namespace k9db

#endif  // K9DB_POLICY_POLICY_ENGINE_H_
