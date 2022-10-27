#include "pelton/shards/sqlengine/tests_helpers.h"

#include <sstream>

#include "pelton/dataflow/record.h"
#include "pelton/shards/sqlengine/engine.h"
#include "pelton/util/status.h"

namespace pelton {
namespace sql {

// Equality modulo order.
bool operator==(const sql::SqlResultSet &set,
                const std::vector<std::string> &vec) {
  std::vector<std::string> v = vec;
  for (const dataflow::Record &r : set.rows()) {
    dataflow::Record copy = r.Copy();
    copy.SetPositive(true);
    std::stringstream ss;
    ss << copy;
    auto it = std::find(v.begin(), v.end(), ss.str());
    if (it == v.end()) {
      return false;
    }
    v.erase(it);
  }
  return v.size() == 0;
}

}  // namespace sql
}  // namespace pelton

namespace pelton {
namespace shards {
namespace sqlengine {

/*
 * Easy creation of sqlast::Statements.
 */

// sqlast::CreateTable
std::string MakeCreate(const std::string &tbl_name,
                       const std::vector<std::string> &cols) {
  std::string sql = "CREATE TABLE " + tbl_name + "(";
  for (size_t i = 0; i < cols.size(); i++) {
    sql += cols.at(i);
    if (i < cols.size() - 1) {
      sql += ",";
    }
  }
  sql += ");";
  return sql;
}

// sqlast::Insert.
std::pair<std::string, std::string> MakeInsert(
    const std::string &tbl_name, const std::vector<std::string> &vals) {
  std::string sql = " INSERT INTO " + tbl_name + " VALUES (";
  for (size_t i = 0; i < vals.size(); i++) {
    sql += vals.at(i);
    if (i < vals.size() - 1) {
      sql += ",";
    }
  }
  sql += ");";

  std::string row = "|";
  for (const std::string &v : vals) {
    if (v[0] == '\'' || v[0] == '"') {
      row += v.substr(1, v.size() - 2);
    } else {
      row += v;
    }
    row += "|";
  }

  return std::pair(sql, row);
}

// sqlast::Delete.
std::string MakeDelete(const std::string &tbl_name,
                       const std::vector<std::string> &conds) {
  std::string sql = "DELETE FROM " + tbl_name;
  if (conds.size() > 0) {
    sql += " WHERE " + conds.front();
    for (size_t i = 1; i < conds.size(); i++) {
      sql += " AND " + conds.at(i);
    }
  }
  return sql + ";";
}

/*
 * Execute a statement.
 */
sql::SqlResult Execute(const std::string &sql, Connection *conn) {
  MOVE_OR_PANIC(sql::SqlResult result, Shard(sql, conn));
  return result;
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  ::gflags::ParseCommandLineFlags(&argc, &argv, true);
  ::google::InitGoogleLogging(argv[0]);

  return RUN_ALL_TESTS();
}
