// INSERT statements sharding and rewriting.

#ifndef PELTON_SHARDS_SQLENGINE_INSERT_H_
#define PELTON_SHARDS_SQLENGINE_INSERT_H_

#include "absl/status/status.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace insert {

absl::Status Shard(const sqlast::Insert &stmt, SharderState *state,
                   dataflow::DataFlowState *dataflow_state,
                   const OutputChannel &output);

}  // namespace insert
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_INSERT_H_
