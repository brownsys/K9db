#include "pelton/sql/lazy_executor.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/shards/types.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

#define DB_NAME "lazy_executor_test"

// Command line flags.
DEFINE_string(db_username, "root", "MariaDB username to connect with");
DEFINE_string(db_password, "password", "MariaDB pwd to connect with");

// Executor is global and used by all tests.
pelton::sql::SqlLazyExecutor executor;
pelton::sql::SqlEagerExecutor *exec;

namespace pelton {
namespace sql {

using CType = sqlast::ColumnDefinition::Type;

// Helpers.

// Unsharded table.
void CreateUnshardedTable() {
  std::string drop = "DROP TABLE IF EXISTS default_db_mytest;";
  EXPECT_TRUE(exec->ExecuteStatement(drop));

  sqlast::CreateTable stmt{"mytest"};
  stmt.AddColumn("id", sqlast::ColumnDefinition("id", CType::INT));
  stmt.AddColumn("name", sqlast::ColumnDefinition("name", CType::TEXT));

  SqlResult result = executor.ExecuteDefault(&stmt);
  EXPECT_TRUE(result.IsStatement());
  EXPECT_TRUE(result.Success());
}
void CleanupUnshardedTable() {
  EXPECT_TRUE(exec->ExecuteStatement("DROP TABLE default_db_mytest;"));
}

// Sharded table.
std::pair<std::string, shards::ShardingInformation> CreateShardedTable(
    const std::string &user_id) {
  // Sharding information.
  shards::ShardingInformation info = {"user", "mytest_user", "user", 1, ""};
  std::string shard_name =
      shards::sqlengine::NameShard(info.shard_kind, user_id);

  // Drop existing table.
  std::string drop = "DROP TABLE IF EXISTS " + shard_name + "_mytest;";
  EXPECT_TRUE(exec->ExecuteStatement(drop));

  // The CreateTable statement.
  sqlast::CreateTable stmt{"mytest"};
  stmt.AddColumn("id", sqlast::ColumnDefinition("id", CType::INT));
  stmt.AddColumn("name", sqlast::ColumnDefinition("name", CType::TEXT));

  // Execute the statement.
  SqlResult result = executor.ExecuteShard(&stmt, info, user_id);
  EXPECT_TRUE(result.IsStatement());
  EXPECT_TRUE(result.Success());

  return std::make_pair(shard_name, info);
}
void CleanupShardedTable(const std::string &user_id) {
  // Sharding information.
  shards::ShardingInformation info = {"user", "mytest_user", "user", 1, ""};
  std::string shard_name =
      shards::sqlengine::NameShard(info.shard_kind, user_id);

  // Drop the table.
  EXPECT_TRUE(exec->ExecuteStatement("DROP TABLE " + shard_name + "_mytest;"));
}

// Schema Creation
dataflow::SchemaRef UnshardedSchema() {
  std::vector<std::string> names = {"ID", "Name"};
  std::vector<CType> types = {CType::INT, CType::TEXT};
  std::vector<dataflow::ColumnID> keys = {0};
  return dataflow::SchemaFactory::Create(names, types, keys);
}
dataflow::SchemaRef ShardedSchema() {
  std::vector<std::string> names = {"ID", "User", "Name"};
  std::vector<CType> types = {CType::INT, CType::INT, CType::TEXT};
  std::vector<dataflow::ColumnID> keys = {0};
  return dataflow::SchemaFactory::Create(names, types, keys);
}

// TESTS
TEST(LazyExecutorTest, TestCreateTableDefault) {
  exec = &executor.eager_executor_;

  // Create table.
  CreateUnshardedTable();

  // Look for default_db_mytest table.
  std::unique_ptr<::sql::ResultSet> tables = exec->ExecuteQuery("SHOW TABLES;");
  bool found = false;
  while (tables->next()) {
    if (tables->getString(1) == "default_db_mytest") {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);

  // Drop mytest table.
  CleanupUnshardedTable();
}

TEST(LazyExecutorTest, TestCreateTableShard) {
  exec = &executor.eager_executor_;

  // Create sharded table.
  auto [shard_name, _] = CreateShardedTable("1");

  // Look for sharded table.
  std::unique_ptr<::sql::ResultSet> tables = exec->ExecuteQuery("SHOW TABLES;");
  bool found = false;
  while (tables->next()) {
    if (tables->getString(1) == shard_name + "_mytest") {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);

  // Drop mytest table.
  CleanupShardedTable("1");
}

TEST(LazyExecutorTest, TestUpdatesDefault) {
  exec = &executor.eager_executor_;

  // Create table.
  CreateUnshardedTable();

  // Insert values.
  sqlast::Insert stmt{"mytest"};
  stmt.SetValues({"1", "\"s1\""});
  SqlResult result = executor.ExecuteDefault(&stmt);
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"2", "\"s2\""});
  result = executor.ExecuteDefault(&stmt);
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"3", "\"s1\""});
  result = executor.ExecuteDefault(&stmt);
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"4", "\"s3\""});
  result = executor.ExecuteDefault(&stmt);
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"5", "\"s2\""});
  result = executor.ExecuteDefault(&stmt);
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  // Update rows.
  std::unique_ptr<sqlast::BinaryExpression> where =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::EQ);
  where->SetLeft(std::make_unique<sqlast::ColumnExpression>("id"));
  where->SetRight(std::make_unique<sqlast::LiteralExpression>("3"));

  sqlast::Update stmt2{"mytest"};
  stmt2.AddColumnValue("name", "\"s5\"");
  stmt2.SetWhereClause(std::move(where));

  result = executor.ExecuteDefault(&stmt2);
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  // Delete rows.
  where =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::EQ);
  where->SetLeft(std::make_unique<sqlast::ColumnExpression>("name"));
  where->SetRight(std::make_unique<sqlast::LiteralExpression>("\"s2\""));

  sqlast::Delete stmt3{"mytest"};
  stmt3.SetWhereClause(std::move(where));

  result = executor.ExecuteDefault(&stmt3);
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 2);

  // Validate data.
  std::unique_ptr<::sql::ResultSet> rows =
      exec->ExecuteQuery("SELECT * FROM default_db_mytest ORDER BY id;");
  EXPECT_TRUE(rows->next());
  EXPECT_EQ(rows->getInt(1), 1);
  EXPECT_EQ(rows->getString(2), "s1");
  EXPECT_TRUE(rows->next());
  EXPECT_EQ(rows->getInt(1), 3);
  EXPECT_EQ(rows->getString(2), "s5");
  EXPECT_TRUE(rows->next());
  EXPECT_EQ(rows->getInt(1), 4);
  EXPECT_EQ(rows->getString(2), "s3");
  EXPECT_FALSE(rows->next());

  // Delete testing table.
  CleanupUnshardedTable();
}

TEST(LazyExecutorTest, TestUpdatesShard) {
  exec = &executor.eager_executor_;

  // Create table.
  auto [shard1, info1] = CreateShardedTable("1");
  auto [shard2, info2] = CreateShardedTable("2");

  // Insert values.
  sqlast::Insert stmt{"mytest"};
  stmt.SetValues({"1", "\"s1\""});
  SqlResult result = executor.ExecuteShard(&stmt, info1, "1");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"2", "\"s2\""});
  result = executor.ExecuteShard(&stmt, info1, "1");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"3", "\"s1\""});
  result = executor.ExecuteShard(&stmt, info2, "2");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"4", "\"s3\""});
  result = executor.ExecuteShard(&stmt, info2, "2");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"5", "\"s2\""});
  result = executor.ExecuteShard(&stmt, info1, "1");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  // Update rows.
  std::unique_ptr<sqlast::BinaryExpression> where =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::EQ);
  where->SetLeft(std::make_unique<sqlast::ColumnExpression>("id"));
  where->SetRight(std::make_unique<sqlast::LiteralExpression>("3"));

  sqlast::Update stmt2{"mytest"};
  stmt2.AddColumnValue("name", "\"s5\"");
  stmt2.SetWhereClause(std::move(where));

  result = executor.ExecuteShard(&stmt2, info1, "1");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 0);
  result = executor.ExecuteShard(&stmt2, info2, "2");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  // Delete rows.
  where =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::EQ);
  where->SetLeft(std::make_unique<sqlast::ColumnExpression>("name"));
  where->SetRight(std::make_unique<sqlast::LiteralExpression>("\"s2\""));

  sqlast::Delete stmt3{"mytest"};
  stmt3.SetWhereClause(std::move(where));

  result = executor.ExecuteShard(&stmt3, info1, "1");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 2);
  result = executor.ExecuteShard(&stmt3, info2, "2");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 0);

  // Validate data.
  std::unique_ptr<::sql::ResultSet> rows =
      exec->ExecuteQuery("SELECT * FROM " + shard1 + "_mytest ORDER BY id;");
  EXPECT_TRUE(rows->next());
  EXPECT_EQ(rows->getInt(1), 1);
  EXPECT_EQ(rows->getString(2), "s1");
  EXPECT_FALSE(rows->next());

  rows = exec->ExecuteQuery("SELECT * FROM " + shard2 + "_mytest ORDER BY id;");
  EXPECT_TRUE(rows->next());
  EXPECT_EQ(rows->getInt(1), 3);
  EXPECT_EQ(rows->getString(2), "s5");
  EXPECT_TRUE(rows->next());
  EXPECT_EQ(rows->getInt(1), 4);
  EXPECT_EQ(rows->getString(2), "s3");
  EXPECT_FALSE(rows->next());

  // Delete all data from all shards.
  sqlast::Delete stmt4{"mytest"};
  result = executor.ExecuteShards(&stmt4, info1, {"1", "2"});
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 3);

  // Delete testing table.
  CleanupShardedTable("1");
  CleanupShardedTable("2");
}

TEST(LazyExecutorTest, TestSelectDefault) {
  exec = &executor.eager_executor_;

  // Create table.
  CreateUnshardedTable();

  // Insert values.
  sqlast::Insert stmt{"mytest"};
  stmt.SetValues({"1", "\"s1\""});
  SqlResult result = executor.ExecuteDefault(&stmt);
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"2", "\"s2\""});
  result = executor.ExecuteDefault(&stmt);
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"3", "\"s1\""});
  result = executor.ExecuteDefault(&stmt);
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"4", "\"s3\""});
  result = executor.ExecuteDefault(&stmt);
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"5", "\"s2\""});
  result = executor.ExecuteDefault(&stmt);
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  // Create Schema and expected records.
  dataflow::SchemaRef schema = UnshardedSchema();
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 1_s, "s1"_uptr);
  records.emplace_back(schema, true, 2_s, "s2"_uptr);
  records.emplace_back(schema, true, 3_s, "s1"_uptr);
  records.emplace_back(schema, true, 4_s, "s3"_uptr);
  records.emplace_back(schema, true, 5_s, "s2"_uptr);

  // Query the database.
  sqlast::Select stmt2{"mytest"};
  stmt2.AddColumn("*");
  result = executor.ExecuteDefault(&stmt2, schema);

  // Test output to make sure it is correct.
  EXPECT_TRUE(result.IsQuery());
  EXPECT_TRUE(result.HasResultSet());

  std::unique_ptr<SqlResultSet> resultset = result.NextResultSet();
  EXPECT_EQ(resultset->GetSchema(), schema);
  for (size_t i = 0; i < records.size(); i++) {
    EXPECT_TRUE(resultset->HasNext());
    EXPECT_EQ(resultset->FetchOne(), records.at(i));
  }
  EXPECT_FALSE(resultset->HasNext());
  EXPECT_FALSE(result.HasResultSet());

  // Clean up.
  CleanupUnshardedTable();
}

TEST(LazyExecutorTest, TestSelectShard) {
  exec = &executor.eager_executor_;

  // Create table.
  auto [shard1, info1] = CreateShardedTable("1");
  auto [shard2, info2] = CreateShardedTable("2");

  // Insert values.
  sqlast::Insert stmt{"mytest"};
  stmt.SetValues({"1", "\"s1\""});
  SqlResult result = executor.ExecuteShard(&stmt, info1, "1");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"2", "\"s2\""});
  result = executor.ExecuteShard(&stmt, info1, "1");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"3", "\"s1\""});
  result = executor.ExecuteShard(&stmt, info2, "2");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"4", "\"s3\""});
  result = executor.ExecuteShard(&stmt, info2, "2");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"5", "\"s2\""});
  result = executor.ExecuteShard(&stmt, info1, "1");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  // Create Schema and expected records.
  dataflow::SchemaRef schema = ShardedSchema();
  std::vector<dataflow::Record> records1, records2;
  records1.emplace_back(schema, false, 1_s, 1_s, "s1"_uptr);
  records1.emplace_back(schema, false, 2_s, 1_s, "s2"_uptr);
  records2.emplace_back(schema, false, 3_s, 2_s, "s1"_uptr);
  records2.emplace_back(schema, false, 4_s, 2_s, "s3"_uptr);
  records1.emplace_back(schema, false, 5_s, 1_s, "s2"_uptr);

  // Query the database for the first shard.
  sqlast::Select stmt2{"mytest"};
  stmt2.AddColumn("*");
  result = executor.ExecuteShard(&stmt2, info1, "1", schema);

  EXPECT_TRUE(result.IsQuery());
  EXPECT_TRUE(result.HasResultSet());
  std::unique_ptr<SqlResultSet> resultset = result.NextResultSet();
  EXPECT_EQ(resultset->GetSchema(), schema);
  for (size_t i = 0; i < records1.size(); i++) {
    EXPECT_TRUE(resultset->HasNext());
    EXPECT_EQ(resultset->FetchOne(), records1.at(i));
  }
  EXPECT_FALSE(resultset->HasNext());
  EXPECT_FALSE(result.HasResultSet());

  // Query the database for the second shard.
  result = executor.ExecuteShard(&stmt2, info2, "2", schema);
  EXPECT_TRUE(result.IsQuery());
  EXPECT_TRUE(result.HasResultSet());
  resultset = result.NextResultSet();
  EXPECT_EQ(resultset->GetSchema(), schema);
  for (size_t i = 0; i < records2.size(); i++) {
    EXPECT_TRUE(resultset->HasNext());
    EXPECT_EQ(resultset->FetchOne(), records2.at(i));
  }
  EXPECT_FALSE(resultset->HasNext());
  EXPECT_FALSE(result.HasResultSet());

  // Clean up.
  CleanupShardedTable("1");
  CleanupShardedTable("2");
}

TEST(LazyExecutorTest, TestSelectShardSet) {
  exec = &executor.eager_executor_;

  // Create table.
  auto [shard1, info1] = CreateShardedTable("1");
  auto [shard2, info2] = CreateShardedTable("2");

  // Insert values.
  sqlast::Insert stmt{"mytest"};
  stmt.SetValues({"1", "\"s1\""});
  SqlResult result = executor.ExecuteShard(&stmt, info1, "1");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"2", "\"s2\""});
  result = executor.ExecuteShard(&stmt, info1, "1");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"3", "\"s1\""});
  result = executor.ExecuteShard(&stmt, info2, "2");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"4", "\"s3\""});
  result = executor.ExecuteShard(&stmt, info2, "2");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  stmt.SetValues({"5", "\"s2\""});
  result = executor.ExecuteShard(&stmt, info1, "1");
  EXPECT_TRUE(result.IsUpdate());
  EXPECT_EQ(result.UpdateCount(), 1);

  // Create Schema and expected records.
  dataflow::SchemaRef schema = ShardedSchema();
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, false, 1_s, 1_s, "s1"_uptr);
  records.emplace_back(schema, false, 2_s, 1_s, "s2"_uptr);
  records.emplace_back(schema, false, 5_s, 1_s, "s2"_uptr);
  records.emplace_back(schema, false, 3_s, 2_s, "s1"_uptr);
  records.emplace_back(schema, false, 4_s, 2_s, "s3"_uptr);

  // Query the database for the first shard.
  sqlast::Select stmt2{"mytest"};
  stmt2.AddColumn("*");
  result = executor.ExecuteShards(&stmt2, info1, {"1", "2"}, schema);

  EXPECT_TRUE(result.IsQuery());
  EXPECT_TRUE(result.HasResultSet());
  std::unique_ptr<SqlResultSet> resultset = result.NextResultSet();
  EXPECT_EQ(resultset->GetSchema(), schema);
  while (records.size() > 0) {
    EXPECT_TRUE(resultset->HasNext());
    dataflow::Record record = resultset->FetchOne();
    auto it = std::find(records.begin(), records.end(), record);
    EXPECT_NE(it, records.end());
    records.erase(it);
  }
  EXPECT_FALSE(resultset->HasNext());
  EXPECT_FALSE(result.HasResultSet());

  // Clean up.
  CleanupShardedTable("1");
  CleanupShardedTable("2");
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
  exec->ExecuteStatement("DROP DATABASE " DB_NAME);

  return result;
}
