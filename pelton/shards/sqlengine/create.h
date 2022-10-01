// CREATE TABLE statements sharding and rewriting.

#ifndef PELTON_SHARDS_SQLENGINE_CREATE_H_
#define PELTON_SHARDS_SQLENGINE_CREATE_H_

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/upgradable_lock.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace create {

absl::StatusOr<sql::SqlResult> Shard(const sqlast::CreateTable &stmt,
                                     Connection *connection,
                                     util::UniqueLock *lock);

}  // namespace create
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_CREATE_H_
