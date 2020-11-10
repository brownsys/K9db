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

#ifndef SHARDS_SQLENGINE_ENGINE_H_
#define SHARDS_SQLENGINE_ENGINE_H_

#include <list>
#include <string>
#include <utility>

#include "shards/state.h"

namespace shards {
namespace sqlengine {

using ShardSuffix = std::string;
using SQLStatement = std::string;

std::list<std::pair<ShardSuffix, SQLStatement>> Rewrite(const std::string &sql,
                                                        SharderState *state);

}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_ENGINE_H_
