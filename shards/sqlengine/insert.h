// INSERT statements sharding and rewriting.

#ifndef SHARDS_SQLENGINE_INSERT_H_
#define SHARDS_SQLENGINE_INSERT_H_

#include <list>
#include <string>
#include <utility>

#include "SQLiteParser.h"
#include "shards/state.h"

namespace shards {
namespace sqlengine {
namespace insert {

std::list<std::pair<std::string, std::string>> Rewrite(
    sqlparser::SQLiteParser::Insert_stmtContext *stmt, SharderState *state);

}  // namespace insert
}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_INSERT_H_
