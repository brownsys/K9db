// Creation and management of dataflows.

#ifndef PELTON_SHARDS_SQLENGINE_VIEW_H_
#define PELTON_SHARDS_SQLENGINE_VIEW_H_

#include "absl/status/statusor.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/types.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace view {

absl::Status CreateView(const sqlast::CreateView &stmt, SharderState *state,
                        dataflow::DataFlowState *dataflow_state);

absl::StatusOr<SqlResult> SelectView(const sqlast::Select &stmt,
                                     SharderState *state,
                                     dataflow::DataFlowState *dataflow_state);

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_VIEW_H_
