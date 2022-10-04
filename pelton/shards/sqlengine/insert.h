// INSERT statements sharding and rewriting.
#ifndef PELTON_SHARDS_SQLENGINE_INSERT_H_
#define PELTON_SHARDS_SQLENGINE_INSERT_H_

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/upgradable_lock.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace insert {

absl::StatusOr<sql::SqlResult> Shard(const sqlast::Insert &stmt,
                                     Connection *connection,
                                     util::SharedLock *lock);

}  // namespace insert
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_INSERT_H_
