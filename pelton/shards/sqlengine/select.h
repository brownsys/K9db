// SELECT statements sharding and rewriting.

#ifndef PELTON_SHARDS_SQLENGINE_SELECT_H_
#define PELTON_SHARDS_SQLENGINE_SELECT_H_

#include <vector>

#include "absl/status/statusor.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/types.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace select {

absl::StatusOr<SqlResult> Shard(const sqlast::Select &stmt, SharderState *state,
                                dataflow::DataFlowState *dataflow_state);

absl::Status Query(std::vector<RawRecord> *output, const sqlast::Select &stmt,
                   SharderState *state, dataflow::DataFlowState *dataflow_state,
                   bool positive);

}  // namespace select
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_SELECT_H_
