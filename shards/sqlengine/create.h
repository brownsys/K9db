// CREATE TABLE statements sharding and rewriting.

#ifndef SHARDS_SQLENGINE_CREATE_H_
#define SHARDS_SQLENGINE_CREATE_H_

#include <list>
#include <string>
#include <utility>

#include "SQLiteParser.h"
#include "shards/state.h"

namespace shards {
namespace sqlengine {
namespace create {

// <ShardSuffix, SQLStatement>
std::list<std::pair<std::string, std::string>> Rewrite(
    sqlparser::SQLiteParser::Create_table_stmtContext *stmt,
    SharderState *state);

}  // namespace create
}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_CREATE_H_
