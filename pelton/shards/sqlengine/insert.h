// INSERT statements sharding and rewriting.

#ifndef PELTON_SHARDS_SQLENGINE_INSERT_H_
#define PELTON_SHARDS_SQLENGINE_INSERT_H_

#include <list>
#include <memory>
#include <string>
#include <tuple>

#include "absl/status/statusor.h"
#include "pelton/shards/sqlexecutor/executable.h"
#include "pelton/shards/state.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace insert {

absl::StatusOr<std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>>
Rewrite(const sqlast::Insert &stmt, SharderState *state);

}  // namespace insert
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_INSERT_H_
