// Delete statements sharding and rewriting.

#ifndef SHARDS_SQLENGINE_DELETE_H_
#define SHARDS_SQLENGINE_DELETE_H_

#include <list>
#include <memory>
#include <string>
#include <tuple>

#include "absl/status/statusor.h"
#include "shards/sqlast/ast.h"
#include "shards/sqlexecutor/executable.h"
#include "shards/state.h"

namespace shards {
namespace sqlengine {
namespace delete_ {

// <ShardSuffix, SQLStatement>
absl::StatusOr<std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>>
Rewrite(const sqlast::Delete &stmt, SharderState *state);

}  // namespace delete_
}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_DELETE_H_
