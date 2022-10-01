// Creation and management of secondary indices.
#ifndef PELTON_SHARDS_SQLENGINE_INDEX_H_
#define PELTON_SHARDS_SQLENGINE_INDEX_H_

#include <string>

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/sql/result.h"
#include "pelton/util/upgradable_lock.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace index {

// SELECT indexed_column, shard_column, COUNT(*)
// FROM indexed_table
// GROUP BY indexed_column, shard_column
// HAVING indexed_column = ?
struct SimpleIndexSpec {
  std::string index_name;
  std::string indexed_table;
  std::string indexed_column;
  std::string shard_column;
};

// SELECT indexed_table.indexed_column, shard_table.shard_column, COUNT(*)
// FROM indexed_table JOIN shard_table
// ON indexed_table.indexed_column = shard_table.join_column
// GROUP BY indexed_table.indexed_column, shard_table.shard_column
// HAVING indexed_table.indexed_column = ?
struct JoinedIndexSpec {
  std::string index_name;
  std::string indexed_table;
  std::string indexed_column;
  std::string shard_table;
  std::string join_column;
  std::string shard_column;
};

absl::StatusOr<sql::SqlResult> CreateIndex(const SimpleIndexSpec &index,
                                           Connection *connection,
                                           util::UniqueLock *lock);

absl::StatusOr<sql::SqlResult> CreateIndex(const JoinedIndexSpec &index,
                                           Connection *connection,
                                           util::UniqueLock *lock);

}  // namespace index
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_INDEX_H_
