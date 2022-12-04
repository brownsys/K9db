// Hardcoded owncloud query.
#ifndef PELTON_SHARDS_SQLENGINE_VIEW_QUERY_H_
#define PELTON_SHARDS_SQLENGINE_VIEW_QUERY_H_

#include <string>

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/sql/result.h"
#include "pelton/util/status.h"
#include "pelton/util/upgradable_lock.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace view {

absl::StatusOr<sql::SqlResult> PerformViewQuery(
    const std::vector<sqlast::Value> params, Connection *conn,
    util::SharedLock *lock);

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_VIEW_QUERY_H_
