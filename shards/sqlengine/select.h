// SELECT statements sharding and rewriting.

#ifndef SHARDS_SQLENGINE_SELECT_H_
#define SHARDS_SQLENGINE_SELECT_H_

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
namespace select {

// <ShardSuffix, SQLStatement>
absl::StatusOr<std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>>
Rewrite(const sqlast::Select &stmt, SharderState *state);

}  // namespace select
}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_SELECT_H_
