// Entry point for our sql rewriting engine.
//
// The engine is responsible for parsing an SQL statement,
// and then rewriting it to be compatible with our sharding DB architecture.
//
// In particular: The engine analyzes CREATE TABLE statements to figure out
// which shard kind the table should belong to, and what its schema should look
// like after sharding. It rewrites INSERT statements to run against the
// relevant shard, and create that shard and its schema on demand. Finally,
// it rewrites SELECT statements into (potentially many) statements that are
// run against the appropriate shards and combined into a query set matching
// the schema of the logical non-sharded DB.

#ifndef PELTON_SHARDS_SQLENGINE_ENGINE_H_
#define PELTON_SHARDS_SQLENGINE_ENGINE_H_

#include <list>
#include <memory>
#include <string>
#include <tuple>

#include "absl/status/statusor.h"
#include "pelton/shards/sqlexecutor/executable.h"
#include "pelton/shards/state.h"

namespace pelton {
namespace shards {
namespace sqlengine {

absl::StatusOr<std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>>
Rewrite(const std::string &sql, SharderState *state);

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_ENGINE_H_