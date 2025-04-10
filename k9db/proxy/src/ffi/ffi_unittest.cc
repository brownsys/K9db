#include "k9db/proxy/src/ffi/ffi.h"

#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"

#define DB_NAME "ffi_test"

FFIConnection c_conn;

// Ensure symbol exists. In reality, this is defined in the proxy.
extern "C" {
void proxy_terminate(int) {}
}

namespace {

using CType = FFIColumnType;

// Drop the database (if it exists).
void DropDatabase() { std::filesystem::remove_all("/tmp/k9db_" DB_NAME); }

// Check that a result has this row (since order can be arbitrary).
bool HasRow(FFIQueryResult result, int id, const std::string &name) {
  for (size_t i = 0; i < FFIResultRowCount(result); i++) {
    if (FFIResultGetInt(result, i, 0) == id && !FFIResultIsNull(result, i, 1) &&
        std::string(FFIResultGetString(result, i, 1)) == name) {
      return true;
    }
  }
  return false;
}
bool HasRowNull(FFIQueryResult result, int id) {
  for (size_t i = 0; i < FFIResultRowCount(result); i++) {
    if (FFIResultGetInt(result, i, 0) == id && FFIResultIsNull(result, i, 1)) {
      return true;
    }
  }
  return false;
}

TEST(PROXY, OPEN_TEST) {
  c_conn = FFIOpen().result;
  EXPECT_NE(c_conn, nullptr);
}

// Test creating tables and views.
TEST(PROXY, DDL_TEST) {
  auto table = "CREATE TABLE test3 (ID int PRIMARY KEY, name VARCHAR(10));";
  EXPECT_TRUE(FFIExecDDL(c_conn, table).result) << "Creating table failed!";

#ifndef K9DB_VALGRIND_MODE
  auto view =
      "CREATE VIEW myview AS "
      "'\"SELECT * FROM test3 WHERE ID > 5 ORDER BY name\"'";
  EXPECT_TRUE(FFIExecDDL(c_conn, view).result) << "Creating view failed!";
#endif
}

// Test insertion/updating/deletion.
TEST(PROXY, UPDATE_TEST) {
  std::vector<std::pair<std::string, int>> tests = {
      {"INSERT INTO test3 VALUES (1, 'hello');", 1},
      {"INSERT INTO test3 VALUES (2, 'bye');", 1},
      {"INSERT INTO test3 VALUES (3, 'world');", 1},
      {"INSERT INTO test3 VALUES (50, NULL);", 1},
      {"UPDATE test3 SET name = 'hi' WHERE ID = 1;", 1},
      {"DELETE FROM test3 WHERE ID = 3;", 1}};
  for (auto &[q, count] : tests) {
    VLOG(1) << "Running query: " << q;
    FFIUpdateResult result = FFIExecUpdate(c_conn, q.c_str()).result;
    EXPECT_EQ(result.row_count, count) << "Failed: " << q;
  }
}

// Test queries.
TEST(PROXY, QUERY_TEST) {
  // Query from table.
  FFIQueryResult response_select =
      FFIExecSelect(c_conn, "SELECT * FROM test3;").result;
  EXPECT_TRUE(FFIResultNextSet(response_select));
  EXPECT_EQ(FFIResultColumnCount(response_select), 2) << "Bad column count";
  EXPECT_EQ(FFIResultRowCount(response_select), 3) << "Bad row count";
  EXPECT_EQ(FFIResultColumnType(response_select, 0), CType::INT) << "Bad types";
  EXPECT_EQ(FFIResultColumnType(response_select, 1), CType::TEXT)
      << "Bad types";
  EXPECT_EQ(std::string(FFIResultColumnName(response_select, 0)), "ID")
      << "Bad schema";
  EXPECT_EQ(std::string(FFIResultColumnName(response_select, 1)), "name")
      << "Bad schema";
  EXPECT_TRUE(HasRow(response_select, 1, "hi"));
  EXPECT_TRUE(HasRow(response_select, 2, "bye"));
  EXPECT_TRUE(HasRowNull(response_select, 50));
  EXPECT_FALSE(FFIResultNextSet(response_select));
  FFIResultDestroy(response_select);

#ifndef K9DB_VALGRIND_MODE
  // Query from view.
  response_select = FFIExecSelect(c_conn, "SELECT * FROM myview;").result;
  EXPECT_TRUE(FFIResultNextSet(response_select));
  EXPECT_EQ(FFIResultColumnCount(response_select), 2) << "Bad column count";
  EXPECT_EQ(FFIResultRowCount(response_select), 1) << "Bad row count";
  EXPECT_EQ(FFIResultColumnType(response_select, 0), CType::INT) << "Bad types";
  EXPECT_EQ(FFIResultColumnType(response_select, 1), CType::TEXT)
      << "Bad types";
  EXPECT_EQ(std::string(FFIResultColumnName(response_select, 0)), "ID")
      << "Bad schema";
  EXPECT_EQ(std::string(FFIResultColumnName(response_select, 1)), "name")
      << "Bad schema";
  EXPECT_EQ(FFIResultGetInt(response_select, 0, 0), 50) << "Bad data";
  EXPECT_EQ(FFIResultIsNull(response_select, 0, 1), true) << "Bad data";
  EXPECT_FALSE(FFIResultNextSet(response_select));
  FFIResultDestroy(response_select);

  // Query using one-off view.
  response_select =
      FFIExecSelect(c_conn, "SELECT * FROM test3 WHERE ID > 4 ORDER BY name;")
          .result;
  EXPECT_TRUE(FFIResultNextSet(response_select));
  EXPECT_EQ(FFIResultColumnCount(response_select), 2) << "Bad column count";
  EXPECT_EQ(FFIResultRowCount(response_select), 1) << "Bad row count";
  EXPECT_EQ(FFIResultColumnType(response_select, 0), CType::INT) << "Bad types";
  EXPECT_EQ(FFIResultColumnType(response_select, 1), CType::TEXT)
      << "Bad types";
  EXPECT_EQ(std::string(FFIResultColumnName(response_select, 0)), "ID")
      << "Bad schema";
  EXPECT_EQ(std::string(FFIResultColumnName(response_select, 1)), "name")
      << "Bad schema";
  EXPECT_EQ(FFIResultGetInt(response_select, 0, 0), 50) << "Bad data";
  EXPECT_EQ(FFIResultIsNull(response_select, 0, 1), true) << "Bad data";
  EXPECT_FALSE(FFIResultNextSet(response_select));
  FFIResultDestroy(response_select);
#endif
}

// Test prepared statements.
#ifndef K9DB_VALGRIND_MODE
TEST(PROXY, PREPARED_STATMENT_TEST) {
  // Reads from table.
  FFIPreparedStatement stmt1 =
      FFIPrepare(c_conn, "SELECT * FROM test3 where ID = ?;").result;

  EXPECT_EQ(FFIPreparedStatementID(stmt1), 0) << "Bad statement id";
  EXPECT_EQ(FFIPreparedStatementArgCount(stmt1), 1) << "Bad arg count";
  EXPECT_EQ(FFIPreparedStatementArgType(stmt1, 0).result, CType::INT)
      << "Arg type";

  FFIPreparedStatement stmt2 =
      FFIPrepare(c_conn, "SELECT * FROM test3 where ID IN (?, ?);").result;
  EXPECT_EQ(FFIPreparedStatementID(stmt2), 1) << "Bad statement id";
  EXPECT_EQ(FFIPreparedStatementArgCount(stmt2), 2) << "Bad arg count";
  EXPECT_EQ(FFIPreparedStatementArgType(stmt2, 0).result, CType::INT)
      << "Arg type";
  EXPECT_EQ(FFIPreparedStatementArgType(stmt2, 1).result, CType::INT)
      << "Arg type";

  FFIPreparedStatement stmt3 =
      FFIPrepare(c_conn,
                 "SELECT * FROM test3 where ID IN (?, ?) AND test3.name = ?;")
          .result;
  EXPECT_EQ(FFIPreparedStatementID(stmt3), 2) << "Bad statement id";
  EXPECT_EQ(FFIPreparedStatementArgCount(stmt3), 3) << "Bad arg count";
  EXPECT_EQ(FFIPreparedStatementArgType(stmt3, 0).result, CType::INT)
      << "Arg type";
  EXPECT_EQ(FFIPreparedStatementArgType(stmt3, 1).result, CType::INT)
      << "Arg type";
  EXPECT_EQ(FFIPreparedStatementArgType(stmt3, 2).result, CType::TEXT)
      << "Arg type";

  // Query from views.
  const char *args1[] = {"1"};
  FFIPreparedResult struct1 = FFIExecPrepare(c_conn, 0, 1, args1).result;
  EXPECT_TRUE(struct1.query);
  FFIQueryResult result1 = struct1.query_result;
  EXPECT_TRUE(FFIResultNextSet(result1));
  EXPECT_EQ(FFIResultColumnCount(result1), 2) << "Bad column count";
  EXPECT_EQ(FFIResultRowCount(result1), 1) << "Bad row count";
  EXPECT_EQ(std::string(FFIResultColumnName(result1, 0)), "ID") << "Schema";
  EXPECT_EQ(std::string(FFIResultColumnName(result1, 1)), "name") << "Schema";
  EXPECT_EQ(FFIResultColumnType(result1, 0), CType::INT) << "Bad types";
  EXPECT_EQ(FFIResultColumnType(result1, 1), CType::TEXT) << "Bad types";
  EXPECT_EQ(FFIResultGetInt(result1, 0, 0), 1) << "Bad data";
  EXPECT_EQ(std::string(FFIResultGetString(result1, 0, 1)), "hi") << "Bad str";
  EXPECT_FALSE(FFIResultNextSet(result1));
  FFIResultDestroy(result1);

  const char *args2[] = {"1", "50"};
  FFIPreparedResult struct2 = FFIExecPrepare(c_conn, 1, 2, args2).result;
  EXPECT_TRUE(struct2.query);
  FFIQueryResult result2 = struct2.query_result;
  EXPECT_TRUE(FFIResultNextSet(result2));
  EXPECT_EQ(FFIResultColumnCount(result2), 2) << "Bad column count";
  EXPECT_EQ(FFIResultRowCount(result2), 2) << "Bad row count";
  EXPECT_EQ(std::string(FFIResultColumnName(result2, 0)), "ID") << "Schema";
  EXPECT_EQ(std::string(FFIResultColumnName(result2, 1)), "name") << "Schema";
  EXPECT_EQ(FFIResultColumnType(result2, 0), CType::INT) << "Bad types";
  EXPECT_EQ(FFIResultColumnType(result2, 1), CType::TEXT) << "Bad types";
  EXPECT_TRUE(HasRow(result2, 1, "hi"));
  EXPECT_TRUE(HasRowNull(result2, 50));
  EXPECT_FALSE(FFIResultNextSet(result2));
  FFIResultDestroy(result2);

  const char *args3[] = {"1", "50", "NULL"};
  FFIPreparedResult struct3 = FFIExecPrepare(c_conn, 2, 3, args3).result;
  EXPECT_TRUE(struct3.query);
  FFIQueryResult result3 = struct3.query_result;
  EXPECT_TRUE(FFIResultNextSet(result3));
  EXPECT_EQ(FFIResultColumnCount(result3), 2) << "Bad column count";
  EXPECT_EQ(FFIResultRowCount(result3), 1) << "Bad row count";
  EXPECT_EQ(std::string(FFIResultColumnName(result3, 0)), "ID") << "Schema";
  EXPECT_EQ(std::string(FFIResultColumnName(result3, 1)), "name") << "Schema";
  EXPECT_EQ(FFIResultColumnType(result3, 0), CType::INT) << "Bad types";
  EXPECT_EQ(FFIResultColumnType(result3, 1), CType::TEXT) << "Bad types";
  EXPECT_EQ(FFIResultGetInt(result3, 0, 0), 50) << "Bad data";
  EXPECT_EQ(FFIResultIsNull(result3, 0, 1), true) << "Bad str";
  EXPECT_FALSE(FFIResultNextSet(result3));
  FFIResultDestroy(result3);

  // Reads from views.
  FFIPreparedStatement stmt4 =
      FFIPrepare(
          c_conn,
          "SELECT ID, COUNT(*) AS C FROM test3 GROUP BY ID HAVING ID = ?;")
          .result;
  EXPECT_EQ(FFIPreparedStatementID(stmt4), 3) << "Bad statement id";
  EXPECT_EQ(FFIPreparedStatementArgCount(stmt4), 1) << "Bad arg count";
  EXPECT_EQ(FFIPreparedStatementArgType(stmt4, 0).result, CType::INT)
      << "Arg type";

  // Test error.
  FFIColumnTypeOrError tmp = FFIPreparedStatementArgType(stmt4, 100);
  EXPECT_NE(tmp.error.code, 0u) << "No error on arg count exceeding parameters";
  EXPECT_NE(tmp.error.message, nullptr)
      << "No error on arg count exceeding parameters";
  FFIFreeError(tmp.error);

  const char *args4[] = {"50"};
  FFIPreparedResult struct4 = FFIExecPrepare(c_conn, 3, 1, args4).result;
  EXPECT_TRUE(struct4.query);
  FFIQueryResult result4 = struct4.query_result;
  EXPECT_TRUE(FFIResultNextSet(result4));
  EXPECT_EQ(FFIResultColumnCount(result4), 2) << "Bad column count";
  EXPECT_EQ(FFIResultRowCount(result4), 1) << "Bad row count";
  EXPECT_EQ(std::string(FFIResultColumnName(result4, 0)), "ID") << "Schema";
  EXPECT_EQ(std::string(FFIResultColumnName(result4, 1)), "C") << "Schema";
  EXPECT_EQ(FFIResultColumnType(result4, 0), CType::INT) << "Bad types";
  EXPECT_EQ(FFIResultColumnType(result4, 1), CType::UINT) << "Bad types";
  EXPECT_EQ(FFIResultGetInt(result4, 0, 0), 50) << "Bad data";
  EXPECT_EQ(FFIResultGetUInt(result4, 0, 1), 1) << "Bad str";
  EXPECT_FALSE(FFIResultNextSet(result4));
  FFIResultDestroy(result4);
}
#endif

TEST(PROXY, CLOSE_TEST) {
  EXPECT_TRUE(FFIClose(c_conn).result) << "Closing connection failed!";
}

}  // namespace

int main(int argc, char **argv) {
  // Command line arugments and help message.
  FFIArgs cmd_args = FFIGflags(argc, argv, "ffi_unittest.cc").result;
  EXPECT_EQ(cmd_args.workers, 1);
  EXPECT_EQ(cmd_args.consistent, true);
  EXPECT_EQ(cmd_args.db_name, std::string("k9db"));
  EXPECT_EQ(cmd_args.hostname, std::string("127.0.0.1:10001"));
  EXPECT_EQ(cmd_args.db_path, std::string(""));

  // Drop the database (in case it existed before because of some tests).
  DropDatabase();

  // Initialize K9db State
  EXPECT_TRUE(FFIInitialize(1, true, DB_NAME, "").result)
      << "Opening global connection failed!";

  // Run tests.
  ::testing::InitGoogleTest(&argc, argv);
  auto result = RUN_ALL_TESTS();

  // Close global connection
  EXPECT_TRUE(FFIShutdown().result) << "Closing global connection failed!";

  // Drop the database again.
  DropDatabase();

  return result;
}
