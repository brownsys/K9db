// Delete statements sharding and rewriting.

#ifndef SHARDS_SQLENGINE_DELETE_H_
#define SHARDS_SQLENGINE_DELETE_H_

#include <list>
#include <string>
#include <tuple>

#include "shards/sqlast/ast.h"
#include "shards/state.h"

namespace shards {
namespace sqlengine {
namespace delete_ {

// <ShardSuffix, SQLStatement>
std::list<std::tuple<std::string, std::string, CallbackModifier>> Rewrite(
    const sqlast::Delete &stmt, SharderState *state);

}  // namespace delete_
}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_DELETE_H_
