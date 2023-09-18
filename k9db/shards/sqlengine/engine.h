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

#ifndef K9DB_SHARDS_SQLENGINE_ENGINE_H_
#define K9DB_SHARDS_SQLENGINE_ENGINE_H_

#include <string>

#include "absl/status/statusor.h"
#include "k9db/connection.h"
#include "k9db/dataflow/state.h"
#include "k9db/shards/state.h"
#include "k9db/sql/result.h"
#include "k9db/sqlast/command.h"

namespace k9db {
namespace shards {
namespace sqlengine {

absl::StatusOr<sql::SqlResult> Shard(const sqlast::SQLCommand &sql,
                                     Connection *connection);
}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db

#endif  // K9DB_SHARDS_SQLENGINE_ENGINE_H_
