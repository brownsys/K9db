// SELECT statements sharding and rewriting.
#include "pelton/shards/sqlengine/select.h"

#include <utility>

#include "pelton/shards/sqlengine/util.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace select {

absl::StatusOr<std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>>
Rewrite(const sqlast::Select &stmt, SharderState *state) {
  sqlast::Stringifier stringifier;
  std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>> result;

  // Table name to select from.
  const std::string &table_name = stmt.table_name();

  // Case 1: table is not in any shard.
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    std::string select_str = stmt.Visit(&stringifier);
    result.push_back(std::make_unique<sqlexecutor::SimpleExecutableStatement>(
        DEFAULT_SHARD_NAME, select_str));
  }

  // Case 2: table is sharded.
  if (is_sharded) {
    // We will query all the different ways of sharding the table (for
    // duplicates with many owners), de-duplication occurs later in the
    // pipeline.
    for (const auto &info : state->GetShardingInformation(table_name)) {
      // Rename the table to match the sharded name.
      sqlast::Select cloned = stmt;
      cloned.table_name() = info.sharded_table_name;

      // Find the value assigned to shard_by column in the where clause, and
      // remove it from the where clause.
      sqlast::ValueFinder value_finder(info.shard_by);
      auto [found, user_id_val] = cloned.Visit(&value_finder);
      if (found) {
        sqlast::ExpressionRemover expression_remover(info.shard_by);
        cloned.Visit(&expression_remover);
      }

      // Select from all the relevant shards.
      std::string select_str = cloned.Visit(&stringifier);
      for (const auto &user_id : state->UsersOfShard(info.shard_kind)) {
        if (found && user_id != user_id_val) {
          continue;
        }

        std::string shard_name = NameShard(info.shard_kind, user_id);
        result.push_back(
            std::make_unique<sqlexecutor::SelectExecutableStatement>(
                shard_name, select_str, info.shard_by_index, info.shard_by,
                user_id));
      }
    }
  }

  return result;
}

}  // namespace select
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
