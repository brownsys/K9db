#include "k9db/policy/policy_engine.h"

#include "glog/logging.h"
#include "k9db/dataflow/state.h"
#include "k9db/policy/policy_builder.h"
#include "k9db/policy/policy_state.h"
#include "k9db/shards/state.h"
#include "k9db/shards/types.h"
#include "k9db/sqlast/ast_policy.h"
#include "k9db/sqlast/ast_value.h"
#include "k9db/util/upgradable_lock.h"

namespace k9db {
namespace policy {

void AddPolicy(const std::string &stmt, Connection *connection) {
  // 0. Acquire global writer lock: this is a schema operation; this is ok!
  util::UniqueLock lock = connection->state->WriterLock();
  shards::SharderState &state = connection->state->SharderState();
  dataflow::DataFlowState &dstate = connection->state->DataflowState();

  // 1. Parse stmt
  sqlast::PolicyStatement parsed = sqlast::PolicyStatement::Parse(stmt);

  // 2. Create any views

  // 3. Add to table.
  CHECK(state.TableExists(parsed.table()));
  shards::Table &table = state.GetTable(parsed.table());
  table.policy_state.AddPolicy(parsed.column(), PolicyBuilder(parsed.schema()));
}

}  // namespace policy
}  // namespace k9db
