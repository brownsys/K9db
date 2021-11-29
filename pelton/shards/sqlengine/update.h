// UPDATE statements sharding and rewriting.

#ifndef PELTON_SHARDS_SQLENGINE_UPDATE_H_
#define PELTON_SHARDS_SQLENGINE_UPDATE_H_

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
namespace update {

absl::StatusOr<sql::SqlResult> Shard(const sqlast::Update &stmt,
                                     Connection *connection);

}  // namespace update
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_UPDATE_H_
