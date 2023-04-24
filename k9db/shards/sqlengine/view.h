// Creation and management of dataflows.
#ifndef K9DB_SHARDS_SQLENGINE_VIEW_H_
#define K9DB_SHARDS_SQLENGINE_VIEW_H_

#include "absl/status/statusor.h"
#include "k9db/connection.h"
#include "k9db/sql/result.h"
#include "k9db/sqlast/ast.h"
#include "k9db/util/upgradable_lock.h"

namespace k9db {
namespace shards {
namespace sqlengine {
namespace view {

absl::StatusOr<sql::SqlResult> CreateView(const sqlast::CreateView &stmt,
                                          Connection *connection,
                                          util::UniqueLock *lock);

absl::StatusOr<sql::SqlResult> SelectView(const sqlast::Select &stmt,
                                          Connection *connection,
                                          util::SharedLock *lock);

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db

#endif  // K9DB_SHARDS_SQLENGINE_VIEW_H_
