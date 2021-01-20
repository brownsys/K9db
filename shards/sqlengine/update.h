// SELECT statements sharding and rewriting.

#ifndef SHARDS_SQLENGINE_UPDATE_H_
#define SHARDS_SQLENGINE_UPDATE_H_

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
namespace update {

// <ShardSuffix, SQLStatement>
absl::StatusOr<std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>>
Rewrite(const sqlast::Update &stmt, SharderState *state);

}  // namespace update
}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_UPDATE_H_
