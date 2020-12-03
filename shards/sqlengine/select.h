// SELECT statements sharding and rewriting.

#ifndef SHARDS_SQLENGINE_SELECT_H_
#define SHARDS_SQLENGINE_SELECT_H_

#include <list>
#include <string>
#include <tuple>

#include "SQLiteParser.h"
#include "shards/state.h"

namespace shards {
namespace sqlengine {
namespace select {

// <ShardSuffix, SQLStatement>
std::list<std::tuple<std::string, std::string, CallbackModifier>> Rewrite(
    sqlparser::SQLiteParser::Select_stmtContext *stmt, SharderState *state);

}  // namespace select
}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_SELECT_H_
