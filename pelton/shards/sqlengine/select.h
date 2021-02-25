// SELECT statements sharding and rewriting.

#ifndef PELTON_SHARDS_SQLENGINE_SELECT_H_
#define PELTON_SHARDS_SQLENGINE_SELECT_H_

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
namespace select {

// <ShardSuffix, SQLStatement>
absl::StatusOr<std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>>
Rewrite(const sqlast::Select &stmt, SharderState *state);

}  // namespace select
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_SELECT_H_
