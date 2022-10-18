// This file contains utility functions, mostly for standarizing
// name (suffixes etc) of things.
#include "pelton/shards/sqlengine/util.h"

#include <algorithm>
#include <memory>
// NOLINTNEXTLINE
#include <string_view>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "pelton/shards/sqlengine/index.h"

namespace pelton {
namespace shards {
namespace sqlengine {

// Determine which shards the given record reside in.
std::vector<ShardLocation> Locate(const std::string &table_name,
                                  const util::ShardName &shard_name,
                                  const std::vector<dataflow::Record> &records,
                                  Connection *conn, util::SharedLock *lock) {
  std::string_view shard_kind = shard_name.ShardKind();
  std::string_view user_id = shard_name.UserID();

  // Get table information.
  const SharderState &sstate = conn->state->SharderState();
  const Table &table = sstate.GetTable(table_name);

  // Store result here.
  std::vector<ShardLocation> result(records.size(), ShardLocation::NO_SHARD);
  for (const std::unique_ptr<ShardDescriptor> &desc : table.owners) {
    bool kind_match = (desc->shard_kind == shard_kind);
    for (size_t i = 0; i < records.size(); i++) {
      if (result.at(i) == ShardLocation::IN_GIVEN_SHARD) {
        continue;
      }
      const dataflow::Record &record = records.at(i);

      // Direct sharding; resolve by directly looking at record.
      if (desc->type == InfoType::DIRECT) {
        const DirectInfo &info = std::get<DirectInfo>(desc->info);
        if (kind_match) {
          if (record.GetValue(info.column_index).AsUnquotedString() ==
              user_id) {
            result.at(i) = ShardLocation::IN_GIVEN_SHARD;
            continue;
          }
        }
        if (!record.IsNull(info.column_index)) {
          result.at(i) = ShardLocation::NOT_IN_GIVEN_SHARD;
        }
        continue;
      }

      // Transitive or variable; resolve via index.
      size_t column;
      const IndexDescriptor *index;
      if (desc->type == InfoType::TRANSITIVE) {
        const auto &info = std::get<TransitiveInfo>(desc->info);
        column = info.column_index;
        index = info.index;
      } else {
        const auto &info = std::get<VariableInfo>(desc->info);
        column = info.column_index;
        index = info.index;
      }

      std::vector<dataflow::Record> shards =
          index::LookupIndex(*index, record.GetValue(column), conn, lock);
      if (shards.size() == 0) {
        continue;
      }
      if (kind_match) {
        bool found = false;
        for (const dataflow::Record &irecord : shards) {
          if (irecord.GetValue(1).AsUnquotedString() == user_id) {
            found = true;
            break;
          }
        }
        if (found) {
          result.at(i) = ShardLocation::IN_GIVEN_SHARD;
          continue;
        }
      }
      result.at(i) = ShardLocation::NOT_IN_GIVEN_SHARD;
    }
  }

  return result;
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
