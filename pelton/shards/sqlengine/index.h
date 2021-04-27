// Creation and management of secondary indices.

#ifndef PELTON_SHARDS_SQLENGINE_INDEX_H_
#define PELTON_SHARDS_SQLENGINE_INDEX_H_

#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace index {

absl::Status CreateIndex(const sqlast::CreateIndex &stmt, SharderState *state,
                         dataflow::DataFlowState *dataflow_state);

}  // namespace index
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_INDEX_H_
