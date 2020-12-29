// INSERT statements sharding and rewriting.

#ifndef SHARDS_SQLENGINE_INSERT_H_
#define SHARDS_SQLENGINE_INSERT_H_

#include <list>
#include <string>
#include <tuple>

#include "shards/sqlast/ast.h"
#include "shards/state.h"

namespace shards {
namespace sqlengine {
namespace insert {

std::list<std::tuple<std::string, std::string, CallbackModifier>> Rewrite(
    const sqlast::Insert &stmt, SharderState *state);

}  // namespace insert
}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_INSERT_H_
