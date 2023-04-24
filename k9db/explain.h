// EXPLAIN COMPLIANCE command.
#ifndef K9DB_EXPLAIN_H_
#define K9DB_EXPLAIN_H_

#include "k9db/connection.h"

namespace k9db {
namespace explain {

void ExplainCompliance(const Connection &connection);

}  // namespace explain
}  // namespace k9db

#endif  // K9DB_EXPLAIN_H_
