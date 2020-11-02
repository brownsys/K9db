// CREATE TABLE statements sharding and rewriting.

#ifndef SHARDS_SQLENGINE_SCHEMA_H_
#define SHARDS_SQLENGINE_SCHEMA_H_

#include "SQLiteParser.h"
#include "shards/state.h"

namespace shards {
namespace sqlengine {
namespace schema {

void Rewrite(sqlparser::SQLiteParser::Create_table_stmtContext *stmt,
             SharderState *state);

}  // namespace schema
}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_SCHEMA_H_
