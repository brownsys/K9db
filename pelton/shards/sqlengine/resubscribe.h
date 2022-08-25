
#ifndef PELTON_SHARDS_SQLENGINE_RESUBSCRIBE_H_
#define PELTON_SHARDS_SQLENGINE_RESUBSCRIBE_H_

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace resubscribe {

absl::StatusOr<sql::SqlResult> Resubscribe(Connection *connection);

}  // namespace resubscribe
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  //  PELTON_SHARDS_SQLENGINE_RESUBSCRIBE_H_
