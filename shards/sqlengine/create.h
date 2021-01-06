// CREATE TABLE statements sharding and rewriting.

#ifndef SHARDS_SQLENGINE_CREATE_H_
#define SHARDS_SQLENGINE_CREATE_H_

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
namespace create {

// <ShardSuffix, SQLStatement>
absl::StatusOr<std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>>
Rewrite(const shards::sqlast::CreateTable &stmt, SharderState *state);

}  // namespace create
}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_CREATE_H_
