// Creation and management of dataflows.

#ifndef PELTON_SHARDS_SQLENGINE_VIEW_H_
#define PELTON_SHARDS_SQLENGINE_VIEW_H_

#include "absl/status/statusor.h"
#include "pelton/dataflow/engine.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace view {

absl::Status CreateView(const sqlast::CreateView &stmt, SharderState *state,
                        dataflow::DataFlowEngine *dataflow_engine);

absl::StatusOr<sql::SqlResult> SelectView(
    const sqlast::Select &stmt, SharderState *state,
    dataflow::DataFlowEngine *dataflow_engine);

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_VIEW_H_
