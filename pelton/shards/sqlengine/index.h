// Creation and management of secondary indices.
#ifndef PELTON_SHARDS_SQLENGINE_INDEX_H_
#define PELTON_SHARDS_SQLENGINE_INDEX_H_

#include <string>

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/sql/result.h"
#include "pelton/util/upgradable_lock.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace index {

absl::StatusOr<sql::SqlResult> CreateIndex(const IndexDescriptor &index,
                                           Connection *connection,
                                           util::UniqueLock *lock);

}  // namespace index
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_INDEX_H_
