#include "mysql_proxy/src/ffi/ffi.h"

#include <filesystem>
#include <memory>
#include <string>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mariadb/conncpp.hpp"

#define DB_NAME "ffi_test"

static std::string *db_username;
static std::string *db_password;

FFIConnection c_conn;

namespace {

using CType = FFIColumnType;

// Drop the database (if it exists).
void DropDatabase() { std::filesystem::remove_all("/tmp/pelton/" DB_NAME); }

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
  EXPECT_EQ(FFIExecUpdate(&c_conn, "INSERT INTO test3 VALUES (1, 'hello');"), 1)
      << "Inserting failed!";
  EXPECT_EQ(FFIExecUpdate(&c_conn, "INSERT INTO test3 VALUES (2, 'bye');"), 1)
      << "Inserting failed!";
  EXPECT_EQ(FFIExecUpdate(&c_conn, "INSERT INTO test3 VALUES (3, 'world');"), 1)
      << "Inserting failed!";
  EXPECT_EQ(FFIExecUpdate(&c_conn, "INSERT INTO test3 VALUES (50, 'hi');"), 1)
      << "Inserting failed!";

  EXPECT_EQ(FFIExecUpdate(&c_conn, "UPDATE test3 SET ID = 10 WHERE ID = 1;"), 1)
      << "Inserting failed!";

  EXPECT_EQ(FFIExecUpdate(&c_conn, "DELETE FROM test3 WHERE ID = 3;"), 1)
      << "Deleting failed!";
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
