// Creation and management of dataflows.

#ifndef PELTON_SHARDS_SQLENGINE_VIEW_H_
#define PELTON_SHARDS_SQLENGINE_VIEW_H_

#include "absl/status/status.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace view {

absl::Status CreateView(const sqlast::CreateView &stmt, SharderState *state,
                        dataflow::DataFlowState *dataflow_state);

absl::Status SelectView(const sqlast::Select &stmt, SharderState *state,
                        dataflow::DataFlowState *dataflow_state,
                        const OutputChannel &output);

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_VIEW_H_
