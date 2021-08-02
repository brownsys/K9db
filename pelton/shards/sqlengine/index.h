// Creation and management of secondary indices.

#ifndef PELTON_SHARDS_SQLENGINE_INDEX_H_
#define PELTON_SHARDS_SQLENGINE_INDEX_H_

#include <string>
#include <unordered_set>
#include <utility>

#include "absl/status/statusor.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace index {

absl::StatusOr<sql::SqlResult> CreateIndex(
    const sqlast::CreateIndex &stmt, SharderState *state,
    dataflow::DataFlowState *dataflow_state);

absl::StatusOr<std::pair<bool, std::unordered_set<UserId>>> LookupIndex(
    const std::string &table_name, const std::string &shard_by,
    const sqlast::BinaryExpression *where_clause, SharderState *state,
    dataflow::DataFlowState *dataflow_state);

absl::StatusOr<std::unordered_set<UserId>> LookupIndex(
    const std::string &index_name, const std::string &value,
    dataflow::DataFlowState *dataflow_state);

}  // namespace index
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_INDEX_H_
