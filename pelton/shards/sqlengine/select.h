// SELECT statements sharding and rewriting.

#ifndef PELTON_SHARDS_SQLENGINE_SELECT_H_
#define PELTON_SHARDS_SQLENGINE_SELECT_H_

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace select {

absl::StatusOr<sql::SqlResult> Shard(const sqlast::Select &stmt,
                                     Connection *connection, bool synchronize,
                                     bool *default_db_hit);

absl::StatusOr<sql::SqlResult> Shard(const sqlast::Select &stmt,
                                     Connection *connection, bool synchronize);

/*!
* Given the select statement for a DP tracker select query and the table scheme for the tracker, return the appropriate SchemaRef.
* @param stmt the address of the statement
* @param table_schema the SchemaRef of the table itself
* @return the SchemaRef of the results.
*/
dataflow::SchemaRef ResultSchemaTracker(const sqlast::Select &stmt,
                                        dataflow::SchemaRef table_schema);

}  // namespace select
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_SELECT_H_
