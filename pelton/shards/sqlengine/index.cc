// Creation and management of secondary indices.
#include "pelton/shards/sqlengine/index.h"

#include "pelton/shards/sqlengine/view.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace index {

std::string EncodeSql(const SimpleIndexSpec &s) {
  std::string sql = "";
  sql += "SELECT " + s.indexed_column + ", " + s.shard_column + ", COUNT(*) ";
  sql += "FROM " + s.indexed_table + " ";
  sql += "GROUP BY " + s.indexed_column + ", " + s.shard_column + " ";
  sql += "HAVING " + s.indexed_column + " = ?";
  return sql;
}

std::string EncodeSql(const JoinedIndexSpec &s) {
  std::string indexed_column = s.indexed_table + "." + s.indexed_column;
  std::string shard_column = s.shard_table + "." + s.shard_column;
  std::string join_column = s.shard_table + "." + s.join_column;
  std::string sql = "";
  sql += "SELECT " + indexed_column + ", " + shard_column + ", COUNT(*) ";
  sql += "FROM " + s.indexed_table + " JOIN " + s.shard_table + " ";
  sql += "ON " + indexed_column + " = " + join_column + " ";
  sql += "GROUP BY " + indexed_column + ", " + shard_column + " ";
  sql += "HAVING " + indexed_column + " = ?";
  return sql;
}

absl::StatusOr<sql::SqlResult> CreateIndex(const SimpleIndexSpec &index,
                                           Connection *connection,
                                           util::UniqueLock *lock) {
  sqlast::CreateView create_stmt(index.index_name, EncodeSql(index));
  return view::CreateView(create_stmt, connection, lock);
}

absl::StatusOr<sql::SqlResult> CreateIndex(const JoinedIndexSpec &index,
                                           Connection *connection,
                                           util::UniqueLock *lock) {
  sqlast::CreateView create_stmt(index.index_name, EncodeSql(index));
  return view::CreateView(create_stmt, connection, lock);
}

}  // namespace index
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
