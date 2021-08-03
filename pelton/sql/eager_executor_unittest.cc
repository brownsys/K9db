#include "pelton/sql/eager_executor.h"

#include <memory>
#include <string>

#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "mariadb/conncpp.hpp"

#define DB_NAME "eager_executor_test"

// Command line flags.
DEFINE_string(db_username, "root", "MariaDB username to connect with");
DEFINE_string(db_password, "password", "MariaDB pwd to connect with");

// Executor is global and used by all tests.
pelton::sql::SqlEagerExecutor executor;

namespace pelton {
namespace sql {

TEST(EagerExecutorTest, TestCreateTable) {
  std::string drop = "DROP TABLE IF EXISTS mytest";
  std::string create =
      "CREATE TABLE mytest (id INT PRIMARY KEY, name VARCHAR(50))";

  EXPECT_TRUE(executor.ExecuteStatement(drop));
  EXPECT_TRUE(executor.ExecuteStatement(create));

  // Look for mytest table.
  std::unique_ptr<::sql::ResultSet> tables =
      executor.ExecuteQuery("SHOW TABLES;");
  bool found = false;
  while (tables->next()) {
    if (tables->getString(1) == "mytest") {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);

  // Drop mytest table.
  EXPECT_TRUE(executor.ExecuteStatement("DROP TABLE mytest;"));
}

TEST(EagerExecutorTest, TestUpdates) {
  std::string drop = "DROP TABLE IF EXISTS mytest";
  std::string create =
      "CREATE TABLE mytest (id INT PRIMARY KEY, name VARCHAR(50))";

  EXPECT_TRUE(executor.ExecuteStatement(drop));
  EXPECT_TRUE(executor.ExecuteStatement(create));

  std::string insert = "INSERT INTO mytest VALUES";
  EXPECT_EQ(executor.ExecuteUpdate(insert + " (1, 'str1')"), 1);
  EXPECT_EQ(executor.ExecuteUpdate(insert + " (2, 'str2')"), 1);
  EXPECT_EQ(executor.ExecuteUpdate(insert + " (3, 'str1')"), 1);
  EXPECT_EQ(executor.ExecuteUpdate(insert + " (4, 'str3')"), 1);
  EXPECT_EQ(executor.ExecuteUpdate(insert + " (5, 'str2')"), 1);

  std::string update1 = "UPDATE mytest SET name = 'str5' WHERE id > 3";
  std::string update2 = "UPDATE mytest SET name = 'str2' WHERE id = 4";
  EXPECT_EQ(executor.ExecuteUpdate(update1), 2);
  EXPECT_EQ(executor.ExecuteUpdate(update2), 1);

  std::string delete1 = "DELETE FROM mytest WHERE name = 'str2'";
  std::string delete2 = "DELETE FROM mytest WHERE id < 4";
  EXPECT_EQ(executor.ExecuteUpdate(delete1), 2);
  EXPECT_EQ(executor.ExecuteUpdate(delete2), 2);

  // Drop mytest table.
  EXPECT_TRUE(executor.ExecuteStatement("DROP TABLE mytest;"));
}

TEST(EagerExecutorTest, TestSelects) {
  std::string drop = "DROP TABLE IF EXISTS mytest";
  std::string create =
      "CREATE TABLE mytest (id INT PRIMARY KEY, name VARCHAR(50))";

  EXPECT_TRUE(executor.ExecuteStatement(drop));
  EXPECT_TRUE(executor.ExecuteStatement(create));

  std::string insert = "INSERT INTO mytest VALUES";
  EXPECT_EQ(executor.ExecuteUpdate(insert + " (1, 'str1')"), 1);
  EXPECT_EQ(executor.ExecuteUpdate(insert + " (2, 'str2')"), 1);
  EXPECT_EQ(executor.ExecuteUpdate(insert + " (3, 'str1')"), 1);
  EXPECT_EQ(executor.ExecuteUpdate(insert + " (4, 'str3')"), 1);
  EXPECT_EQ(executor.ExecuteUpdate(insert + " (5, 'str2')"), 1);

  std::string query1 = "SELECT * FROM mytest WHERE name = 'str2' ORDER BY id";
  std::unique_ptr<::sql::ResultSet> result = executor.ExecuteQuery(query1);
  EXPECT_TRUE(result->next());
  EXPECT_EQ(result->getInt(1), 2);
  EXPECT_EQ(result->getString(2), "str2");
  EXPECT_TRUE(result->next());
  EXPECT_EQ(result->getInt(1), 5);
  EXPECT_EQ(result->getString(2), "str2");
  EXPECT_FALSE(result->next());

  std::string query2 = "SELECT * FROM mytest WHERE id > 3 ORDER BY id";
  result = executor.ExecuteQuery(query2);
  EXPECT_TRUE(result->next());
  EXPECT_EQ(result->getInt(1), 4);
  EXPECT_EQ(result->getString(2), "str3");
  EXPECT_TRUE(result->next());
  EXPECT_EQ(result->getInt(1), 5);
  EXPECT_EQ(result->getString(2), "str2");
  EXPECT_FALSE(result->next());

  // Drop mytest table.
  EXPECT_TRUE(executor.ExecuteStatement("DROP TABLE mytest;"));
}

}  // namespace sql
}  // namespace pelton

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Command line arugments and help message.
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Connect to DB.
  const std::string &db_username = FLAGS_db_username;
  const std::string &db_password = FLAGS_db_password;

  // Initialize the executor being tested.
  executor.Initialize(DB_NAME, db_username, db_password);

  // Run tests.
  // Connection is closed implicitly when executor is destructed.
  auto result = RUN_ALL_TESTS();

  // DROP test DB.
  executor.ExecuteStatement("DROP DATABASE " DB_NAME);

  return result;
}
