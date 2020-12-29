// INSERT statements sharding and rewriting.

#include "shards/sqlengine/insert.h"

#include <list>

#include "shards/sqlengine/util.h"

namespace shards {
namespace sqlengine {
namespace insert {

std::list<std::tuple<std::string, std::string, CallbackModifier>> Rewrite(
    const sqlast::Insert &stmt, SharderState *state) {
  // Make sure table exists in the schema first.
  const std::string &table_name = stmt.table_name();
  if (!state->Exists(table_name)) {
    throw "Table does not exist!";
  }

  sqlast::Stringifier stringifier;
  std::list<std::tuple<std::string, std::string, CallbackModifier>> result;

  // Case 1: table is not in any shard.
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    // The insertion statement is unmodified.
    std::string insert_str = stmt.Visit(&stringifier);
    result.emplace_back(DEFAULT_SHARD_NAME, insert_str, identity_modifier);
  }

  // Case 2: table is sharded!
  if (is_sharded) {
    // Duplicate the value for every shard this table has.
    for (const ShardingInformation &sharding_info :
         state->GetShardingInformation(table_name)) {
      sqlast::Insert cloned = stmt;
      cloned.table_name() = sharding_info.sharded_table_name;
      // Find the value corresponding to the shard by column.
      std::string value;
      if (cloned.HasColumns()) {
        value = cloned.RemoveValue(sharding_info.shard_by);
      } else {
        value = cloned.RemoveValue(sharding_info.shard_by_index);
      }

      // TODO(babman): better to do this after user insert rather than user data
      //               insert.
      std::string shard_name = NameShard(sharding_info.shard_kind, value);
      if (!state->ShardExists(sharding_info.shard_kind, value)) {
        for (auto create_stmt :
             state->CreateShard(sharding_info.shard_kind, value)) {
          result.emplace_back(shard_name, create_stmt, identity_modifier);
        }
      }

      // Add the modified insert statement.
      std::string insert_stmt_str = cloned.Visit(&stringifier);
      result.emplace_back(shard_name, insert_stmt_str, identity_modifier);
    }
  }
  return result;
}

}  // namespace insert
}  // namespace sqlengine
}  // namespace shards
