// GDPR statements handling (FORGET and GET).

#include "pelton/shards/sqlengine/gdpr.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace gdpr {

absl::StatusOr<mysql::SqlResult> Shard(
    const sqlast::GDPRStatement &stmt, SharderState *state,
    dataflow::DataFlowState *dataflow_state) {
  perf::Start("GDPR");
  mysql::SqlResult result;

  if (stmt.operation() == sqlast::GDPRStatement::Operation::GET) {
    std::cout << "GET" << std::endl;
  } else {
    std::cout << "FORGET" << std::endl;
  }
  std::cout << stmt.shard_kind() << " :: " << stmt.user_id() << std::endl;

  perf::End("GDPR");
  return result;
}

}  // namespace gdpr
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
