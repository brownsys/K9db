#include "pelton/proxy/src/ffi/ffi.h"

#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mariadb/conncpp.hpp"

#define DB_NAME "ffi_test"

FFIConnection c_conn;

namespace {

using CType = FFIColumnType;

// Drop the database (if it exists).
void DropDatabase() {
  std::filesystem::remove_all("/tmp/pelton/rocksdb/" DB_NAME);
}

TEST(PROXY, OPEN_TEST) {
  c_conn = FFIOpen(DB_NAME);
  EXPECT_TRUE(c_conn.connected) << "Opening connection failed!";
}

// Test creating tables and views.
TEST(PROXY, DDL_TEST) {
  auto table = "CREATE TABLE test3 (ID int PRIMARY KEY, name VARCHAR(10));";
  EXPECT_TRUE(FFIExecDDL(&c_conn, table)) << "Creating table failed!";

#ifndef PELTON_VALGRIND_MODE
  auto view =
      "CREATE VIEW myview AS "
      "'\"SELECT * FROM test3 WHERE ID > 5 ORDER BY name\"'";
  EXPECT_TRUE(FFIExecDDL(&c_conn, view)) << "Creating view failed!";
#endif
}

// Test insertion/updating/deletion.
TEST(PROXY, UPDATE_TEST) {
  std::vector<std::pair<std::string, int>> tests = {
      {"INSERT INTO test3 VALUES (1, 'hello');", 1},
      {"INSERT INTO test3 VALUES (2, 'bye');", 1},
      {"INSERT INTO test3 VALUES (3, 'world');", 1},
      {"INSERT INTO test3 VALUES (50, 'hi');", 1},
      {"UPDATE test3 SET ID = 10 WHERE ID = 1;", 1},
      {"DELETE FROM test3 WHERE ID = 3;", 1}};
  for (auto &[q, count] : tests) {
    VLOG(1) << "Running query: " << q;
    EXPECT_EQ(FFIExecUpdate(&c_conn, q.c_str()), count)
        << "Query \"" << q << "\" failed";
  }
}

// Test queries.
TEST(PROXY, QUERY_TEST) {
  // Query from table.
  FFIResult *response_select = FFIExecSelect(&c_conn, "SELECT * FROM test3;");
  EXPECT_EQ(response_select->num_cols, 2) << "Bad column count";
  EXPECT_EQ(response_select->num_rows, 3) << "Bad row count";
  EXPECT_EQ(std::string(response_select->col_names[0]), "ID") << "Bad schema";
  EXPECT_EQ(std::string(response_select->col_names[1]), "name") << "Bad schema";
  EXPECT_EQ(response_select->col_types[0], CType::INT) << "Bad types";
  EXPECT_EQ(response_select->col_types[1], CType::TEXT) << "Bad types";
  EXPECT_EQ(response_select->records[0].record.INT, 10) << "Bad data";
  EXPECT_EQ(std::string(response_select->records[1].record.TEXT), "hello")
      << "Bad data";
  EXPECT_EQ(response_select->records[2].record.INT, 2) << "Bad data";
  EXPECT_EQ(std::string(response_select->records[3].record.TEXT), "bye")
      << "Bad data";
  EXPECT_EQ(response_select->records[4].record.INT, 50) << "Bad data";
  EXPECT_EQ(std::string(response_select->records[5].record.TEXT), "hi")
      << "Bad data";
  free(response_select->col_names[0]);
  free(response_select->col_names[1]);
  free(response_select->records[1].record.TEXT);
  free(response_select->records[3].record.TEXT);
  free(response_select->records[5].record.TEXT);
  FFIDestroySelect(response_select);

#ifndef PELTON_VALGRIND_MODE
  // Query from view.
  response_select = FFIExecSelect(&c_conn, "SELECT * FROM myview;");
  EXPECT_EQ(response_select->num_cols, 2) << "Bad column count";
  EXPECT_EQ(response_select->num_rows, 2) << "Bad row count";
  EXPECT_EQ(std::string(response_select->col_names[0]), "ID") << "Bad schema";
  EXPECT_EQ(std::string(response_select->col_names[1]), "name") << "Bad schema";
  EXPECT_EQ(response_select->col_types[0], CType::INT) << "Bad types";
  EXPECT_EQ(response_select->col_types[1], CType::TEXT) << "Bad types";
  EXPECT_EQ(response_select->records[0].record.INT, 50) << "Bad data";
  EXPECT_EQ(std::string(response_select->records[1].record.TEXT), "hi")
      << "Bad data";
  EXPECT_EQ(response_select->records[2].record.INT, 10) << "Bad data";
  EXPECT_EQ(std::string(response_select->records[3].record.TEXT), "hello")
      << "Bad data";
  free(response_select->col_names[0]);
  free(response_select->col_names[1]);
  free(response_select->records[1].record.TEXT);
  free(response_select->records[3].record.TEXT);
  FFIDestroySelect(response_select);
#endif
}

// Test prepared statements.
#ifndef PELTON_VALGRIND_MODE
TEST(PROXY, PREPARED_STATMENT_TEST) {
  // Query from table.
  FFIPreparedStatement *stmt1 =
      FFIPrepare(&c_conn, "SELECT * FROM test3 where ID = ?;");
  EXPECT_EQ(stmt1->stmt_id, 0) << "Bad statement id";
  EXPECT_EQ(stmt1->arg_count, 1) << "Bad arg count";
  EXPECT_EQ(stmt1->args[0], CType::INT) << "Bad arg type";
  FFIPreparedStatement *stmt2 =
      FFIPrepare(&c_conn, "SELECT * FROM test3 where ID IN (?, ?);");
  EXPECT_EQ(stmt2->stmt_id, 1) << "Bad statement id";
  EXPECT_EQ(stmt2->arg_count, 2) << "Bad arg count";
  EXPECT_EQ(stmt2->args[0], CType::INT) << "Bad arg type";
  EXPECT_EQ(stmt2->args[1], CType::INT) << "Bad arg type";
  FFIPreparedStatement *stmt3 = FFIPrepare(
      &c_conn, "SELECT * FROM test3 where ID IN (?, ?) AND test3.name = ?;");
  EXPECT_EQ(stmt3->stmt_id, 2) << "Bad statement id";
  EXPECT_EQ(stmt3->arg_count, 3) << "Bad arg count";
  EXPECT_EQ(stmt3->args[0], CType::INT) << "Bad arg type";
  EXPECT_EQ(stmt3->args[1], CType::INT) << "Bad arg type";
  EXPECT_EQ(stmt3->args[2], CType::TEXT) << "Bad arg type";
  FFIDestroyPreparedStatement(stmt1);
  FFIDestroyPreparedStatement(stmt2);
  FFIDestroyPreparedStatement(stmt3);

  // Query from views.
  const char *args1[] = {"10"};
  FFIResult *result1 = FFIExecPrepare(&c_conn, 0, 1, args1);
  EXPECT_EQ(result1->num_cols, 2) << "Bad column count";
  EXPECT_EQ(result1->num_rows, 1) << "Bad row count";
  EXPECT_EQ(std::string(result1->col_names[0]), "ID") << "Bad schema";
  EXPECT_EQ(std::string(result1->col_names[1]), "name") << "Bad schema";
  EXPECT_EQ(result1->col_types[0], CType::INT) << "Bad types";
  EXPECT_EQ(result1->col_types[1], CType::TEXT) << "Bad types";
  EXPECT_EQ(result1->records[0].record.INT, 10) << "Bad data";
  EXPECT_EQ(std::string(result1->records[1].record.TEXT), "hello") << "Bad str";
  free(result1->col_names[0]);
  free(result1->col_names[1]);
  free(result1->records[1].record.TEXT);
  FFIDestroySelect(result1);

  const char *args2[] = {"10", "50"};
  FFIResult *result2 = FFIExecPrepare(&c_conn, 1, 2, args2);
  EXPECT_EQ(result2->num_cols, 2) << "Bad column count";
  EXPECT_EQ(result2->num_rows, 2) << "Bad row count";
  EXPECT_EQ(std::string(result2->col_names[0]), "ID") << "Bad schema";
  EXPECT_EQ(std::string(result2->col_names[1]), "name") << "Bad schema";
  EXPECT_EQ(result2->col_types[0], CType::INT) << "Bad types";
  EXPECT_EQ(result2->col_types[1], CType::TEXT) << "Bad types";
  EXPECT_EQ(result2->records[0].record.INT, 10) << "Bad data";
  EXPECT_EQ(std::string(result2->records[1].record.TEXT), "hello") << "Bad str";
  EXPECT_EQ(result2->records[2].record.INT, 50) << "Bad data";
  EXPECT_EQ(std::string(result2->records[3].record.TEXT), "hi") << "Bad str";
  free(result2->col_names[0]);
  free(result2->col_names[1]);
  free(result2->records[1].record.TEXT);
  free(result2->records[3].record.TEXT);
  FFIDestroySelect(result2);

  const char *args3[] = {"10", "50", "'hi'"};
  FFIResult *result3 = FFIExecPrepare(&c_conn, 2, 3, args3);
  EXPECT_EQ(result3->num_cols, 2) << "Bad column count";
  EXPECT_EQ(result3->num_rows, 1) << "Bad row count";
  EXPECT_EQ(std::string(result3->col_names[0]), "ID") << "Bad schema";
  EXPECT_EQ(std::string(result3->col_names[1]), "name") << "Bad schema";
  EXPECT_EQ(result3->col_types[0], CType::INT) << "Bad types";
  EXPECT_EQ(result3->col_types[1], CType::TEXT) << "Bad types";
  EXPECT_EQ(result3->records[0].record.INT, 50) << "Bad data";
  EXPECT_EQ(std::string(result3->records[1].record.TEXT), "hi") << "Bad str";
  free(result3->col_names[0]);
  free(result3->col_names[1]);
  free(result3->records[1].record.TEXT);
  FFIDestroySelect(result3);
}
#endif

TEST(PROXY, CLOSE_TEST) {
  EXPECT_TRUE(FFIClose(&c_conn)) << "Closing connection failed!";
}

}  // namespace

int main(int argc, char **argv) {
  // Command line arugments and help message.
  FFIArgs cmd_args = FFIGflags(argc, argv, "ffi_unittest.cc");

  // Drop the database (in case it existed before because of some tests).
  DropDatabase();

  // Initialize Pelton State
  EXPECT_TRUE(FFIInitialize(3, true)) << "Opening global connection failed!";

  // Run tests.
  ::testing::InitGoogleTest(&argc, argv);
  auto result = RUN_ALL_TESTS();

  // Close global connection
  EXPECT_TRUE(FFIShutdown()) << "Closing global connection failed!";

  // Drop the database again.
  DropDatabase();

  return result;
}
