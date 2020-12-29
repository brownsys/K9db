// SELECT statements sharding and rewriting.
#include "shards/sqlengine/select.h"

#include <utility>

#include "shards/sqlengine/util.h"

namespace shards {
namespace sqlengine {
namespace select {

std::list<std::tuple<std::string, std::string, CallbackModifier>> Rewrite(
    const sqlast::Select &stmt, SharderState *state) {
  sqlast::Stringifier stringifier;
  std::list<std::tuple<std::string, std::string, CallbackModifier>> result;

  // Table name to select from.
  const std::string &table_name = stmt.table_name();

  // Case 1: table is not in any shard.
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    std::string select_str = stmt.Visit(&stringifier);
    result.emplace_back(DEFAULT_SHARD_NAME, select_str, identity_modifier);
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

      // Select from all the shards.
      std::string select_str = cloned.Visit(&stringifier);
      char *user_id_col_name = new char[info.shard_by.size() + 1];
      memcpy(user_id_col_name, info.shard_by.c_str(), info.shard_by.size());
      user_id_col_name[info.shard_by.size()] = 0;
      int user_id_col_index = info.shard_by_index;

      for (const auto &user_id : state->UsersOfShard(info.shard_kind)) {
        if (found && user_id != user_id_val) {
          continue;
        }

        char *user_id_data = new char[user_id.size() + 1];
        memcpy(user_id_data, user_id.c_str(), user_id.size());
        user_id_data[user_id.size()] = 0;

        std::string shard_name = NameShard(info.shard_kind, user_id);
        result.emplace_back(
            shard_name, select_str,
            [=](int *col_count, char ***col_data, char ***col_names) {
              (*col_count)++;
              char **new_data = new char *[*col_count];
              char **new_name = new char *[*col_count];
              new_data[user_id_col_index] = user_id_data;
              new_name[user_id_col_index] = user_id_col_name;
              for (int i = 0; i < user_id_col_index; i++) {
                new_data[i] = (*col_data)[i];
                new_name[i] = (*col_names)[i];
              }
              for (int i = user_id_col_index + 1; i < *col_count; i++) {
                new_data[i] = (*col_data)[i - 1];
                new_name[i] = (*col_names)[i - 1];
              }
              *col_data = new_data;
              *col_names = new_name;
              identity_modifier(col_count, col_data, col_names);
            });
      }
    }
  }

  return result;
}

}  // namespace select
}  // namespace sqlengine
}  // namespace shards
