// Delete statements sharding and rewriting.

#include "shards/sqlengine/delete.h"

#include <cstdio>
#include <utility>

#include "absl/strings/str_cat.h"
#include "shards/sqlengine/util.h"

namespace shards {
namespace sqlengine {
namespace delete_ {

absl::StatusOr<std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>>
Rewrite(const sqlast::Delete &stmt, SharderState *state) {
  std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>> result;

  const std::string &table_name = stmt.table_name();
  bool is_sharded = state->IsSharded(table_name);
  bool is_pii = state->IsPII(table_name);
  sqlast::Stringifier stringifier;

  // Case 1: Table has PII.
  if (is_pii) {
    sqlast::ValueFinder value_finder(state->PkOfPII(table_name));

    // We are deleting a user, we should also delete their shard!
    auto [found, user_id] = stmt.Visit(&value_finder);
    if (!found) {
      throw "DELETE PII statement does not specify an exact user to delete!";
    }

    // Remove user shard.
    std::string shard = sqlengine::NameShard(table_name, user_id);
    std::string path = absl::StrCat(state->dir_path(), shard, ".sqlite3");
    remove(path.c_str());
    state->RemoveUserFromShard(table_name, user_id);

    // Turn the delete statement back to a string, to delete relevant row in
    // PII table.
    std::string delete_str = stmt.Visit(&stringifier);
    result.push_back(std::make_unique<sqlexecutor::SimpleExecutableStatement>(
        DEFAULT_SHARD_NAME, delete_str));
  }

  // Case 2: Table does not have PII and is not sharded!
  if (!is_sharded && !is_pii) {
    std::string delete_str = stmt.Visit(&stringifier);
    result.push_back(std::make_unique<sqlexecutor::SimpleExecutableStatement>(
        DEFAULT_SHARD_NAME, delete_str));
  }

  // Case 3: Table is sharded!
  if (is_sharded) {
    throw "Unsupported case for DELETE statement!";
  }

  return result;
}

}  // namespace delete_
}  // namespace sqlengine
}  // namespace shards
