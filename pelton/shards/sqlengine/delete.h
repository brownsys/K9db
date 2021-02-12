// Delete statements sharding and rewriting.

#ifndef PELTON_SHARDS_SQLENGINE_DELETE_H_
#define PELTON_SHARDS_SQLENGINE_DELETE_H_

#include <list>
#include <memory>
#include <string>
#include <tuple>

#include "absl/status/statusor.h"
#include "pelton/shards/sqlexecutor/executable.h"
#include "pelton/shards/state.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace delete_ {

// <ShardSuffix, SQLStatement>
absl::StatusOr<std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>>
Rewrite(const sqlast::Delete &stmt, SharderState *state);

}  // namespace delete_
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_DELETE_H_
