// CREATE TABLE statements sharding and rewriting.

#ifndef SHARDS_SQLENGINE_CREATE_H_
#define SHARDS_SQLENGINE_CREATE_H_

#include <list>
#include <string>
#include <tuple>

#include "shards/sqlast/ast.h"
#include "shards/state.h"

namespace shards {
namespace sqlengine {
namespace create {

// <ShardSuffix, SQLStatement>
std::list<std::tuple<std::string, std::string, CallbackModifier>> Rewrite(
    const shards::sqlast::CreateTable &stmt, SharderState *state);

}  // namespace create
}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_CREATE_H_
