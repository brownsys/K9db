#include "mysql_proxy/src/ffi/ffi.h"

#include <memory>
#include <string>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mariadb/conncpp.hpp"

// Read DB configurations.
// Command line flags.
DEFINE_string(db_username, "root", "MariaDB username to connect with");
DEFINE_string(db_password, "password", "MariaDB pwd to connect with");

static std::string *db_username;
static std::string *db_password;

FFIConnection c_conn;

namespace {

using CType = FFIColumnType;

void DropDatabase() {
  sql::ConnectOptionsMap connection_properties;
  connection_properties["hostName"] = "localhost";
  connection_properties["userName"] = *db_username;
  connection_properties["password"] = *db_password;

  sql::Driver *driver = sql::mariadb::get_driver_instance();
  std::unique_ptr<sql::Connection> conn =
      std::unique_ptr<sql::Connection>(driver->connect(connection_properties));
  std::unique_ptr<sql::Statement> stmt =
      std::unique_ptr<sql::Statement>(conn->createStatement());

  stmt->execute("DROP DATABASE IF EXISTS pelton");
}

TEST(PROXY, OPEN_TEST) {
  c_conn = FFIOpen("", db_username->c_str(), db_password->c_str());
  EXPECT_TRUE(c_conn.connected) << "Openning connection failed!";
}

// Test creating tables and views.
TEST(PROXY, DDL_TEST) {
  auto table = "CREATE TABLE test3 (ID int, name VARCHAR(10));";
  EXPECT_TRUE(FFIExecDDL(&c_conn, table)) << "Creating table failed!";

#ifndef PELTON_VALGRIND_MODE
  auto view = "CREATE VIEW myview AS '\"SELECT * FROM test3 WHERE ID > 5\"'";
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
  EXPECT_EQ(response_select->values[0].INT, 10) << "Bad data";
  EXPECT_EQ(std::string(response_select->values[1].TEXT), "hello")
      << "Bad data";
  EXPECT_EQ(response_select->values[2].INT, 2) << "Bad data";
  EXPECT_EQ(std::string(response_select->values[3].TEXT), "bye") << "Bad data";
  EXPECT_EQ(response_select->values[4].INT, 50) << "Bad data";
  EXPECT_EQ(std::string(response_select->values[5].TEXT), "hi") << "Bad data";
  free(response_select->col_names[0]);
  free(response_select->col_names[1]);
  free(response_select->values[1].TEXT);
  free(response_select->values[3].TEXT);
  free(response_select->values[5].TEXT);
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
  EXPECT_EQ(response_select->values[0].INT, 50) << "Bad data";
  EXPECT_EQ(std::string(response_select->values[1].TEXT), "hi") << "Bad data";
  EXPECT_EQ(response_select->values[2].INT, 10) << "Bad data";
  EXPECT_EQ(std::string(response_select->values[3].TEXT), "hello")
      << "Bad data";
  free(response_select->col_names[0]);
  free(response_select->col_names[1]);
  free(response_select->values[1].TEXT);
  free(response_select->values[3].TEXT);
  FFIDestroySelect(response_select);
#endif
}

TEST(PROXY, CLOSE_TEST) {
  EXPECT_TRUE(FFIClose(&c_conn)) << "Closing connection failed!";
}

}  // namespace

int main(int argc, char **argv) {
  // Command line arugments and help message.
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  db_username = new std::string(FLAGS_db_username);
  db_password = new std::string(FLAGS_db_password);

  // Initialize Google’s logging library.
  google::InitGoogleLogging("proxy_unittest");

  // Drop the database (in case it existed before because of some tests).
  DropDatabase();

  // Run tests.
  ::testing::InitGoogleTest(&argc, argv);
  auto result = RUN_ALL_TESTS();

  // Drop the database again.
  DropDatabase();

  return result;
}
