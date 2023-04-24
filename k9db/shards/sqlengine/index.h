// Creation and management of secondary indices.
#ifndef K9DB_SHARDS_SQLENGINE_INDEX_H_
#define K9DB_SHARDS_SQLENGINE_INDEX_H_

#include <string>
#include <unordered_set>
#include <vector>

#include "absl/status/statusor.h"
#include "k9db/connection.h"
#include "k9db/dataflow/record.h"
#include "k9db/shards/types.h"
#include "k9db/sqlast/ast.h"
#include "k9db/util/shard_name.h"
#include "k9db/util/upgradable_lock.h"

namespace k9db {
namespace shards {
namespace sqlengine {
namespace index {

// Index lookup: value must be unparsed: i.e. surrounded by quotes etc.
std::vector<dataflow::Record> LookupIndex(const IndexDescriptor &index,
                                          sqlast::Value &&value,
                                          Connection *connection,
                                          util::SharedLock *lock);

// Index creation.
absl::StatusOr<IndexDescriptor> Create(const std::string &table_name,
                                       const std::string &shard_kind,
                                       const std::string &column_name,
                                       Connection *connection,
                                       util::UniqueLock *lock);

}  // namespace index
}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db

#endif  // K9DB_SHARDS_SQLENGINE_INDEX_H_
