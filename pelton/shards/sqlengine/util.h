// This file contains utility functions, mostly for standarizing
// name (suffixes etc) of things.
#ifndef PELTON_SHARDS_SQLENGINE_UTIL_H_
#define PELTON_SHARDS_SQLENGINE_UTIL_H_

#include <string>
#include <vector>

#include "pelton/connection.h"
#include "pelton/dataflow/record.h"
#include "pelton/util/shard_name.h"
#include "pelton/util/upgradable_lock.h"

namespace pelton {
namespace shards {
namespace sqlengine {

// Determine which shards the given record reside in.
enum class ShardLocation { NO_SHARD, NOT_IN_GIVEN_SHARD, IN_GIVEN_SHARD };

std::vector<ShardLocation> Locate(const std::string &table_name,
                                  const util::ShardName &shard_name,
                                  const std::vector<dataflow::Record> &records,
                                  Connection *conn, util::SharedLock *lock);

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_UTIL_H_
