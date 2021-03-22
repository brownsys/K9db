// UPDATE statements sharding and rewriting.
#include "pelton/shards/sqlengine/update.h"

#include <utility>

#include "pelton/shards/sqlengine/util.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace update {

absl::StatusOr<std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>>
Rewrite(const sqlast::Update &stmt, SharderState *state) {
  sqlast::Stringifier stringifier;
  std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>> result;

  // Table name to select from.
  const std::string &table_name = stmt.table_name();

  // Case 1: table is not in any shard.
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    std::string update_str = stmt.Visit(&stringifier);
    result.push_back(std::make_unique<sqlexecutor::SimpleExecutableStatement>(
        DEFAULT_SHARD_NAME, update_str));
  }

  // Case 2: table is sharded.
  if (is_sharded) {
    // The table might be sharded according to different column/owners.
    // We must update all these different duplicates.
    for (const auto &info : state->GetShardingInformation(table_name)) {
      if (stmt.AssignsTo(info.shard_by)) {
        return absl::InvalidArgumentError("Update cannot modify owner column");
      }

      // Rename the table to match the sharded name.
      sqlast::Update cloned = stmt;
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
      std::string update_str = cloned.Visit(&stringifier);
      for (const auto &user_id : state->UsersOfShard(info.shard_kind)) {
        if (found && user_id != user_id_val) {
          continue;
        }

        std::string shard_name = NameShard(info.shard_kind, user_id);
        result.push_back(
            std::make_unique<sqlexecutor::SimpleExecutableStatement>(
                shard_name, update_str));
      }
    }
  }

  return result;
}

}  // namespace update
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
