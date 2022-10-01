// Creation and management of secondary indices.

#ifndef PELTON_SHARDS_SQLENGINE_INDEX_H_
#define PELTON_SHARDS_SQLENGINE_INDEX_H_

#include <string>
#include <unordered_set>
#include <utility>

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

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

/*
absl::StatusOr<sql::SqlResult> CreateIndex(const sqlast::CreateIndex &stmt,
                                           Connection *connection);

absl::StatusOr<sql::SqlResult> CreateIndexStateIsAlreadyLocked(
    const sqlast::CreateIndex &stmt, Connection *connection, UniqueLock *lock);

absl::StatusOr<std::pair<bool, std::unordered_set<UserId>>> LookupIndex(
    const std::string &table_name, const std::string &shard_by,
    const sqlast::BinaryExpression *where_clause, Connection *connection);

absl::StatusOr<std::unordered_set<UserId>> LookupIndex(
    const std::string &index_name, const std::string &value,
    Connection *connection);

absl::StatusOr<RecordStatus> RecordStatusForUser(const std::string &index_name,
                                                 const std::string &value,
                                                 const std::string &user_id,
                                                 Connection *connection);
*/

}  // namespace index
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLENGINE_INDEX_H_
