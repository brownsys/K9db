// Delete statements sharding and rewriting.

#include "pelton/shards/sqlengine/delete.h"

#include <cstdio>
#include <utility>

#include "absl/strings/str_cat.h"
#include "pelton/shards/sqlengine/util.h"

namespace pelton {
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
      return absl::InvalidArgumentError(
          "DELETE PII statement does not specify an exact user to delete!");
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
    // The table might be sharded according to different column/owners.
    // We must delete from all these different duplicates.
    for (const auto &info : state->GetShardingInformation(table_name)) {
      // Rename the table to match the sharded name.
      sqlast::Delete cloned = stmt;
      cloned.table_name() = info.sharded_table_name;

      // Find the value assigned to shard_by column in the where clause, and
      // remove it from the where clause.
      sqlast::ValueFinder value_finder(info.shard_by);
      auto [found, user_id_val] = cloned.Visit(&value_finder);
      if (found) {
        sqlast::ExpressionRemover expression_remover(info.shard_by);
        cloned.Visit(&expression_remover);
      }

      // Update against the relevant shards.
      std::string delete_str = cloned.Visit(&stringifier);
      for (const auto &user_id : state->UsersOfShard(info.shard_kind)) {
        if (found && user_id != user_id_val) {
          continue;
        }

        std::string shard_name = NameShard(info.shard_kind, user_id);
        result.push_back(
            std::make_unique<sqlexecutor::SimpleExecutableStatement>(
                shard_name, delete_str));
      }
    }
  }

  return result;
}

}  // namespace delete_
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
