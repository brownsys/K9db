// SELECT statements sharding and rewriting.

#ifndef PELTON_SHARDS_SQLENGINE_UPDATE_H_
#define PELTON_SHARDS_SQLENGINE_UPDATE_H_

#include "absl/status/status.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace update {

absl::Status Shard(const sqlast::Update &stmt, SharderState *state,
                   dataflow::DataFlowState *dataflow_state);

}  // namespace update
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_UPDATE_H_
