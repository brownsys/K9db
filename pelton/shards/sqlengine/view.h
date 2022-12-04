// Creation and management of dataflows.
#ifndef PELTON_SHARDS_SQLENGINE_VIEW_H_
#define PELTON_SHARDS_SQLENGINE_VIEW_H_

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/upgradable_lock.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace view {

absl::StatusOr<sql::SqlResult> CreateView(const sqlast::CreateView &stmt,
                                          Connection *connection,
                                          util::UniqueLock *lock, bool index);

absl::StatusOr<sql::SqlResult> SelectView(const sqlast::Select &stmt,
                                          Connection *connection,
                                          util::SharedLock *lock);

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_VIEW_H_
