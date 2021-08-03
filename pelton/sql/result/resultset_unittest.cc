#include "pelton/sql/result/resultset.h"

#include <memory>
#include <string>
#include <utility>

#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "mariadb/conncpp.hpp"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sql/eager_executor.h"
#include "pelton/util/ints.h"

#define DB_NAME "resultset_test"

// Command line flags.
DEFINE_string(db_username, "root", "MariaDB username to connect with");
DEFINE_string(db_password, "password", "MariaDB pwd to connect with");

// Executor is global and used by all tests.
pelton::sql::SqlEagerExecutor executor;

void SetupDB(const std::string &db_username, const std::string &db_password) {
  executor.Initialize(DB_NAME, db_username, db_password);

  std::string drop = "DROP TABLE IF EXISTS mytest";
  std::string create =
      "CREATE TABLE mytest (id INT PRIMARY KEY, name VARCHAR(50))";

  EXPECT_TRUE(executor.ExecuteStatement(drop));
  EXPECT_TRUE(executor.ExecuteStatement(create));

  std::string insert = "INSERT INTO mytest VALUES";
  EXPECT_EQ(executor.ExecuteUpdate(insert + " (1, 's1')"), 1);
  EXPECT_EQ(executor.ExecuteUpdate(insert + " (2, 's2')"), 1);
  EXPECT_EQ(executor.ExecuteUpdate(insert + " (3, 's1')"), 1);
  EXPECT_EQ(executor.ExecuteUpdate(insert + " (4, 's3')"), 1);
  EXPECT_EQ(executor.ExecuteUpdate(insert + " (5, 's2')"), 1);
}

void CleanupDB() {
  EXPECT_TRUE(executor.ExecuteStatement("DROP TABLE mytest;"));
  EXPECT_TRUE(executor.ExecuteStatement("DROP DATABASE " DB_NAME));
}

namespace pelton {
namespace sql {
namespace _result {

using CType = sqlast::ColumnDefinition::Type;

TEST(ResultSetTest, InlineResultSet) {
  // Create the schema.
  std::vector<std::string> names = {"ID", "Name"};
  std::vector<CType> types = {CType::INT, CType::TEXT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Create a bunch of records.
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 1_s, "s1"_uptr);
  records.emplace_back(schema, true, 2_s, "s2"_uptr);
  records.emplace_back(schema, true, 3_s, "s1"_uptr);
  records.emplace_back(schema, true, 4_s, "s3"_uptr);
  records.emplace_back(schema, true, 5_s, "s2"_uptr);

  // Make a copy.
  std::vector<dataflow::Record> copies;
  for (const dataflow::Record &copy : records) {
    copies.push_back(copy.Copy());
  }

  // Create the result set.
  SqlInlineResultSet resultset{schema, std::move(copies)};

  // Test resultset content.
  EXPECT_TRUE(resultset.IsInline());
  for (size_t i = 0; i < records.size(); i++) {
    EXPECT_TRUE(resultset.HasNext());
    EXPECT_EQ(resultset.FetchOne(), records.at(i));
  }
  EXPECT_FALSE(resultset.HasNext());
}

TEST(ResultSetTest, SimpleLazyResultSet) {
  // Create the schema.
  std::vector<std::string> names = {"ID", "Name"};
  std::vector<CType> types = {CType::INT, CType::TEXT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Create a bunch of records.
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 1_s, "s1"_uptr);
  records.emplace_back(schema, true, 2_s, "s2"_uptr);
  records.emplace_back(schema, true, 3_s, "s1"_uptr);
  records.emplace_back(schema, true, 4_s, "s3"_uptr);
  records.emplace_back(schema, true, 5_s, "s2"_uptr);

  // Create resultset.
  std::string query = "SELECT * FROM mytest ORDER BY id";
  SqlLazyResultSet resultset{schema, &executor};
  resultset.AddShardResult({query, {}});

  // Test resultset content.
  EXPECT_FALSE(resultset.IsInline());
  for (size_t i = 0; i < records.size(); i++) {
    EXPECT_TRUE(resultset.HasNext());
    EXPECT_EQ(resultset.FetchOne(), records.at(i));
  }
  EXPECT_FALSE(resultset.HasNext());
}

TEST(ResultSetTest, MultiLazyResultSet) {
  // Create the schema.
  std::vector<std::string> names = {"ID", "Name"};
  std::vector<CType> types = {CType::INT, CType::TEXT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Create a bunch of records.
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 1_s, "s1"_uptr);
  records.emplace_back(schema, true, 2_s, "s2"_uptr);
  records.emplace_back(schema, true, 3_s, "s1"_uptr);
  records.emplace_back(schema, true, 4_s, "s3"_uptr);
  records.emplace_back(schema, true, 5_s, "s2"_uptr);

  // Create resultset.
  std::string query1 = "SELECT * FROM mytest WHERE id < 3 ORDER BY id";
  std::string query2 = "SELECT * FROM mytest WHERE id >= 3 ORDER BY id";
  SqlLazyResultSet resultset{schema, &executor};
  resultset.AddShardResult({query1, {}});
  resultset.AddShardResult({query2, {}});

  // Test resultset content.
  EXPECT_FALSE(resultset.IsInline());
  for (size_t i = 0; i < records.size(); i++) {
    EXPECT_TRUE(resultset.HasNext());
    EXPECT_EQ(resultset.FetchOne(), records.at(i));
  }
  EXPECT_FALSE(resultset.HasNext());
}

TEST(ResultSetTest, AugmentedLazyResultSet) {
  // Create the schema.
  std::vector<std::string> names = {"ID", "Name", "Value"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::UINT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Create a bunch of records.
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 1_u, "s1"_uptr, 10_u);
  records.emplace_back(schema, true, 2_u, "s2"_uptr, 10_u);
  records.emplace_back(schema, true, 3_u, "s1"_uptr, 50_u);
  records.emplace_back(schema, true, 4_u, "s3"_uptr, 50_u);
  records.emplace_back(schema, true, 5_u, "s2"_uptr, 50_u);

  // Create resultset.
  std::string query1 = "SELECT * FROM mytest WHERE id < 3 ORDER BY id";
  std::string query2 = "SELECT * FROM mytest WHERE id >= 3 ORDER BY id";
  SqlLazyResultSet resultset{schema, &executor};
  resultset.AddShardResult({query1, {{2, "10"}}});
  resultset.AddShardResult({query2, {{2, "50"}}});

  // Test resultset content.
  EXPECT_FALSE(resultset.IsInline());
  for (size_t i = 0; i < records.size(); i++) {
    EXPECT_TRUE(resultset.HasNext());
    EXPECT_EQ(resultset.FetchOne(), records.at(i));
  }
  EXPECT_FALSE(resultset.HasNext());
}

TEST(ResultSetTest, DeduplicateLazyResultSet) {
  // Create the schema.
  std::vector<std::string> names = {"ID", "Name", "Value"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::UINT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Create a bunch of records.
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 1_u, "s1"_uptr, 10_u);
  records.emplace_back(schema, true, 2_u, "s2"_uptr, 10_u);
  records.emplace_back(schema, true, 3_u, "s1"_uptr, 50_u);
  records.emplace_back(schema, true, 4_u, "s3"_uptr, 50_u);
  records.emplace_back(schema, true, 5_u, "s2"_uptr, 50_u);
  records.emplace_back(schema, true, 1_u, "s1"_uptr, 90_u);
  records.emplace_back(schema, true, 2_u, "s2"_uptr, 90_u);

  std::string query1 = "SELECT * FROM mytest WHERE id < 3 ORDER BY id";
  std::string query2 = "SELECT * FROM mytest WHERE id >= 3 ORDER BY id";

  // Create resultsets.
  SqlLazyResultSet resultset{schema, &executor};
  resultset.AddShardResult({query1, {{2, "10"}}});
  resultset.AddShardResult({query2, {{2, "50"}}});

  std::unique_ptr<SqlLazyResultSet> resultset2 =
      std::make_unique<SqlLazyResultSet>(schema, &executor);
  resultset2->AddShardResult({query1, {{2, "90"}}});
  resultset2->AddShardResult({query2, {{2, "50"}}});

  // Merge Deduplicate.
  resultset.Append(std::move(resultset2), true);

  // Test resultset content.
  EXPECT_FALSE(resultset.IsInline());
  for (size_t i = 0; i < records.size(); i++) {
    EXPECT_TRUE(resultset.HasNext());
    EXPECT_EQ(resultset.FetchOne(), records.at(i));
  }
  EXPECT_FALSE(resultset.HasNext());
}

TEST(ResultSetTest, AppendLazyResultSet) {
  // Create the schema.
  std::vector<std::string> names = {"ID", "Name", "Value"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::UINT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Create a bunch of records.
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 1_u, "s1"_uptr, 10_u);
  records.emplace_back(schema, true, 2_u, "s2"_uptr, 10_u);
  records.emplace_back(schema, true, 3_u, "s1"_uptr, 50_u);
  records.emplace_back(schema, true, 4_u, "s3"_uptr, 50_u);
  records.emplace_back(schema, true, 5_u, "s2"_uptr, 50_u);
  records.emplace_back(schema, true, 1_u, "s1"_uptr, 90_u);
  records.emplace_back(schema, true, 2_u, "s2"_uptr, 90_u);
  records.emplace_back(schema, true, 3_u, "s1"_uptr, 50_u);
  records.emplace_back(schema, true, 4_u, "s3"_uptr, 50_u);
  records.emplace_back(schema, true, 5_u, "s2"_uptr, 50_u);

  std::string query1 = "SELECT * FROM mytest WHERE id < 3 ORDER BY id";
  std::string query2 = "SELECT * FROM mytest WHERE id >= 3 ORDER BY id";

  // Create resultsets.
  SqlLazyResultSet resultset{schema, &executor};
  resultset.AddShardResult({query1, {{2, "10"}}});
  resultset.AddShardResult({query2, {{2, "50"}}});

  std::unique_ptr<SqlLazyResultSet> resultset2 =
      std::make_unique<SqlLazyResultSet>(schema, &executor);
  resultset2->AddShardResult({query1, {{2, "90"}}});
  resultset2->AddShardResult({query2, {{2, "50"}}});

  // Merge Deduplicate.
  resultset.Append(std::move(resultset2), false);

  // Test resultset content.
  EXPECT_FALSE(resultset.IsInline());
  for (size_t i = 0; i < records.size(); i++) {
    EXPECT_TRUE(resultset.HasNext());
    EXPECT_EQ(resultset.FetchOne(), records.at(i));
  }
  EXPECT_FALSE(resultset.HasNext());
}

}  // namespace _result
}  // namespace sql
}  // namespace pelton

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Command line arugments and help message.
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Connect to DB.
  const std::string &db_username = FLAGS_db_username;
  const std::string &db_password = FLAGS_db_password;

  // Initialize the DB for testing.
  SetupDB(db_username, db_password);

  // Run tests.
  int result = RUN_ALL_TESTS();

  // Clean up the database.
  CleanupDB();

  return result;
}
