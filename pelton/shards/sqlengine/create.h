// CREATE TABLE statements sharding and rewriting.

#ifndef PELTON_SHARDS_SQLENGINE_CREATE_H_
#define PELTON_SHARDS_SQLENGINE_CREATE_H_

#include "absl/status/statusor.h"
#include "pelton/dataflow/engine.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace create {

absl::StatusOr<sql::SqlResult> Shard(const sqlast::CreateTable &stmt,
                                     SharderState *state,
                                     dataflow::DataFlowEngine *dataflow_engine);

}  // namespace create
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_CREATE_H_
