#ifndef PELTON_SHARDS_SQLENGINE_UTIL_H_
#define PELTON_SHARDS_SQLENGINE_UTIL_H_

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/dataflow/record.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/util/shard_name.h"
#include "pelton/util/upgradable_lock.h"

namespace pelton {
namespace shards {
namespace sqlengine {

using ShardsSet = std::unordered_set<util::ShardName>;

// Responsible for cascading effects that copy/remove/move records between
// shards in dependent tables.
class Cascader {
 public:
  Cascader(Connection *conn, util::SharedLock *lock)
      : conn_(conn),
        sstate_(conn->state->SharderState()),
        dstate_(conn->state->DataflowState()),
        db_(conn->session.get()),
        lock_(lock) {}

  // Represents a "WHERE <column> IN <values>" condition.
  struct Condition {
    size_t column;
    std::vector<sqlast::Value> values;
  };

  // Insert the records matched by condition in that given table to the given
  // shards and recurse on their dependent tables.
  // Affected records that were stored in the default shard are moved and not
  // copied.
  absl::StatusOr<int> CascadeTo(const std::string &table_name,
                                const std::string &shard_kind,
                                const ShardsSet &shards,
                                const Condition &condition);

  // Remove the records matched by condition in that given table to the given
  // shards and recurse on their dependent tables.
  // Records that would not be stored in any shard after removal are moved to
  // the default shard.
  absl::StatusOr<int> CascadeOut(const std::string &table_name,
                                 const util::ShardName &shard,
                                 const Condition &condition);
  absl::StatusOr<int> CascadeOut(const std::string &table_name,
                                 const std::string &shard_kind,
                                 const ShardsSet &shards,
                                 const Condition &condition);

  // Locates the legitimate shards of the given records.
  absl::StatusOr<std::vector<ShardsSet>> LocateShards(
      const std::string &table_name,
      const std::vector<dataflow::Record> &records) const;

  // Locates the shards that contain records that match the given condition.
  absl::Status LocateShards(const std::string &table_name,
                            const Condition &condition,
                            std::vector<ShardsSet> *output) const;

 private:
  // Pelton connection.
  Connection *conn_;

  // Connection components.
  SharderState &sstate_;
  dataflow::DataFlowState &dstate_;
  sql::Session *db_;

  // Shared Lock so we can read from the states safetly.
  util::SharedLock *lock_;

  // Keep track of records moved to the default shard to avoid moving them
  // again.
  std::unordered_map<std::string, std::unordered_set<std::string>> moved_;
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_UTIL_H_
