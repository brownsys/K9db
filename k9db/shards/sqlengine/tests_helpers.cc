#include "k9db/shards/sqlengine/tests_helpers.h"

#include <sstream>

#include "k9db/dataflow/record.h"
#include "k9db/shards/sqlengine/engine.h"
#include "k9db/sqlast/command.h"

namespace k9db {
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

bool operator==(const std::vector<sql::SqlResultSet> &left,
                const std::vector<std::vector<std::string>> &right) {
  std::vector<std::vector<std::string>> right_copy = right;
  for (const sql::SqlResultSet &item : left) {
    auto it = std::find(right_copy.begin(), right_copy.end(), item);
    if (it == right_copy.end()) {
      return false;
    }
    right_copy.erase(it);
  }
  return right_copy.size() == 0;
}

}  // namespace sql
}  // namespace k9db

namespace k9db {
namespace shards {
namespace sqlengine {

/*
 * Easy creation of sqlast::Statements.
 */

// sqlast::CreateTable
std::string MakeCreate(const std::string &tbl_name,
                       const std::vector<std::string> &cols, bool data_subject,
                       std::string anon_str) {
  std::string sql;
  if (data_subject) {
    sql = "CREATE DATA_SUBJECT TABLE " + tbl_name + "(";
  } else {
    sql = "CREATE TABLE " + tbl_name + "(";
  }
  for (size_t i = 0; i < cols.size(); i++) {
    sql += cols.at(i);
    if (i < cols.size() - 1) {
      sql += ", ";
    }
  }
  sql += anon_str + ");";
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

std::pair<std::string, std::string> MakeReplace(
    const std::string &tbl_name, const std::vector<std::string> &vals) {
  std::string sql = " REPLACE INTO " + tbl_name + " VALUES (";
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

// sqlast::Update.
std::string MakeUpdate(
    const std::string &tbl_name,
    const std::vector<std::pair<std::string, std::string>> &set_pairs,
    const std::vector<std::string> &conds) {
  std::string sql = "UPDATE " + tbl_name + " SET ";

  for (size_t i = 0; i < set_pairs.size(); i++) {
    sql += set_pairs.at(i).first + " = " + set_pairs.at(i).second;
    if (i < set_pairs.size() - 1) {
      sql += ", ";
    }
  }

  if (conds.size() > 0) {
    sql += " WHERE ";
    for (size_t i = 0; i < conds.size(); i++) {
      sql += conds.at(i);
      if (i < conds.size() - 1) {
        sql += " AND ";
      }
    }
  }

  sql += ";";
  return sql;
}

// sqlast::Get.
std::string MakeGDPRGet(const std::string &tbl_name,
                        const std::string &user_id) {
  std::string sql = "GDPR GET " + tbl_name + " " + user_id + ";";
  return sql;
}

// sqlast::Forget.
std::string MakeGDPRForget(const std::string &tbl_name,
                           const std::string &user_id) {
  std::string sql = "GDPR FORGET " + tbl_name + " " + user_id + ";";
  return sql;
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
  sqlast::SQLCommand command(sql);
  MOVE_OR_PANIC(sql::SqlResult result, Shard(command, conn));
  return result;
}

/*
 * Execute expecting an error.
 */
bool ExecuteError(const std::string &sql, Connection *conn) {
  sqlast::SQLCommand command(sql);
  absl::StatusOr<sql::SqlResult> result = Shard(command, conn);
  if (!result.ok()) {
    conn->session->RollbackTransaction();
    PANIC(conn->ctx->RollbackCheckpoint());
    return true;
  }
  return false;
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  ::gflags::ParseCommandLineFlags(&argc, &argv, true);
  ::google::InitGoogleLogging(argv[0]);

  return RUN_ALL_TESTS();
}
