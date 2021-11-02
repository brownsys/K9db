// Creation and management of dataflows.

#ifndef PELTON_SHARDS_SQLENGINE_VIEW_H_
#define PELTON_SHARDS_SQLENGINE_VIEW_H_

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
namespace view {

absl::Status CreateView(const sqlast::CreateView &stmt, Connection *connection);

absl::StatusOr<sql::SqlResult> SelectView(const sqlast::Select &stmt,
                                          Connection *connection);

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_VIEW_H_
