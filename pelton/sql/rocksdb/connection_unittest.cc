#include "pelton/sql/rocksdb/connection.h"

#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "pelton/sqlast/ast.h"
#include "pelton/sqlast/parser.h"

// Mut be included after parser to avoid duplicating FAIL() definition as macro.
// NOLINTNEXTLINE
#include "glog/logging.h"
// NOLINTNEXTLINE
#include "gtest/gtest.h"

#define DB_NAME "test"
#define DB_PATH "/tmp/pelton/rocksdb/test"

namespace pelton {
namespace sql {

namespace {
using CType = sqlast::ColumnDefinition::Type;
typedef uint32_t ColumnID;
// Drop the database (if it exists).
void DropDatabase() { std::filesystem::remove_all(DB_PATH); }

std::unique_ptr<sqlast::AbstractStatement> Parse(const std::string &sql) {
  sqlast::SQLParser parser;
  // std::unique_ptr<sqlast::AbstractStatement> statement,
  auto statusor = parser.Parse(sql);
  EXPECT_TRUE(statusor.ok()) << statusor.status();
  return std::move(statusor.value());
}

void Print(std::vector<dataflow::Record> &&records,
           const dataflow::SchemaRef &schema) {
  std::cout << schema << std::endl;
  for (auto &r : records) {
    std::cout << r << std::endl;
  }
  std::cout << std::endl;
}
void CheckEqual(const std::vector<dataflow::Record> *a,
                const std::vector<dataflow::Record *> b) {
  assert((*a).size() == b.size());
  for (size_t i = 0; i < b.size(); i++) {
    assert(std::count((*a).begin(), (*a).end(), *b[i]));
  }
}  // namespace

TEST(RocksdbConnectionTest, NoData) {
  DropDatabase();

  RocksdbConnection conn;
  conn.Open(DB_NAME);
  // Create table.
  auto parsed =
      Parse("CREATE TABLE users(PII_id int PRIMARY KEY, name VARCHAR(50));");
  std::cout << conn.ExecuteCreateTable(
                   *static_cast<sqlast::CreateTable *>(parsed.get()))
            << std::endl;
  dataflow::SchemaRef schema1 = dataflow::SchemaFactory::Create(
      *static_cast<sqlast::CreateTable *>(parsed.get()));
  parsed = Parse(
      "CREATE TABLE stories(id int PRIMARY KEY, story_name VARCHAR(50), author "
      "VARCHAR(50), FOREIGN KEY (author) REFERENCES users(name));");
  std::cout << conn.ExecuteCreateTable(
                   *static_cast<sqlast::CreateTable *>(parsed.get()))
            << std::endl;
  dataflow::SchemaRef schema2 = dataflow::SchemaFactory::Create(
      *static_cast<sqlast::CreateTable *>(parsed.get()));
  parsed = Parse("CREATE INDEX idx ON stories (story_name);");
  std::cout << "test 1" << std::endl;
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
  std::cout << "test 2" << std::endl;
  parsed = Parse("SELECT * FROM stories WHERE story_name = 'test';");
  auto resultset1 = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset1.Vec(), schema2);
  parsed = Parse("INSERT INTO users VALUES(1, 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(10, 'A Story', 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed =
      Parse("SELECT id, author FROM stories WHERE story_name in ('K', 'Kk');");
  auto resultset2 = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset2.Vec(), schema2);
  assert(resultset1.empty());
  assert(resultset2.empty());
  conn.Close();
}
TEST(RocksdbConnectionTest, MultipleShardsAndRows) {
  DropDatabase();

  RocksdbConnection conn;
  conn.Open(DB_NAME);
  // Create table.
  auto parsed =
      Parse("CREATE TABLE users(PII_id int PRIMARY KEY, name VARCHAR(50));");
  std::cout << conn.ExecuteCreateTable(
                   *static_cast<sqlast::CreateTable *>(parsed.get()))
            << std::endl;
  dataflow::SchemaRef schema1 = dataflow::SchemaFactory::Create(
      *static_cast<sqlast::CreateTable *>(parsed.get()));
  parsed = Parse(
      "CREATE TABLE stories(id int PRIMARY KEY, story_name VARCHAR(50), author "
      "VARCHAR(50), FOREIGN KEY (author) REFERENCES users(name));");
  std::cout << conn.ExecuteCreateTable(
                   *static_cast<sqlast::CreateTable *>(parsed.get()))
            << std::endl;
  dataflow::SchemaRef schema2 = dataflow::SchemaFactory::Create(
      *static_cast<sqlast::CreateTable *>(parsed.get()));
  parsed = Parse("CREATE INDEX idx ON stories (story_name);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
  parsed = Parse("INSERT INTO users VALUES(1, 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO users (PII_id, name) VALUES(2, 'MALTE');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO users (name, PII_id) VALUES('ISHAN', 3);");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "3")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(1, 'K', 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(2, 'K', 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(3, 'K', 'MALTE');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(4, 'Kk', 'MALTE');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("SELECT * FROM stories WHERE story_name = 'K';");
  auto resultset = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset.Vec(), schema2);
  pelton::dataflow::Record record1{schema2};
  int64_t v0 = 1;
  std::unique_ptr<std::string> ptr1 = std::make_unique<std::string>("K");
  std::string *v1 = ptr1.get();  // Does not release ownership.
  std::unique_ptr<std::string> ptr2 = std::make_unique<std::string>("KINAN");
  std::string *v2 = ptr2.get();  // Does not release ownership.
  record1.SetData(v0, std::move(ptr1), std::move(ptr2));
  pelton::dataflow::Record record2{schema2};
  v0 = 2;
  ptr1 = std::make_unique<std::string>("K");
  v1 = ptr1.get();  // Does not release ownership.
  ptr2 = std::make_unique<std::string>("KINAN");
  v2 = ptr2.get();  // Does not release ownership.
  record2.SetData(v0, std::move(ptr1), std::move(ptr2));
  pelton::dataflow::Record record3{schema2};
  v0 = 3;
  ptr1 = std::make_unique<std::string>("K");
  v1 = ptr1.get();  // Does not release ownership.
  ptr2 = std::make_unique<std::string>("MALTE");
  v2 = ptr2.get();  // Does not release ownership.
  record3.SetData(v0, std::move(ptr1), std::move(ptr2));
  std::vector<dataflow::Record *> ans = {&record1, &record2, &record3};
  CheckEqual(&resultset.rows(), ans);
  // assert(std::count(resultset.rows().begin(), resultset.rows().end(),
  // record1)); ADD ASSERT STATEMENTS FOR REMAINING TESTS
  conn.Close();
}
TEST(RocksdbConnectionTest, MultipleIndexesExist) {
  DropDatabase();

  RocksdbConnection conn;
  conn.Open(DB_NAME);
  // Create table.
  auto parsed =
      Parse("CREATE TABLE users(PII_id int PRIMARY KEY, name VARCHAR(50));");
  std::cout << conn.ExecuteCreateTable(
                   *static_cast<sqlast::CreateTable *>(parsed.get()))
            << std::endl;
  dataflow::SchemaRef schema1 = dataflow::SchemaFactory::Create(
      *static_cast<sqlast::CreateTable *>(parsed.get()));
  parsed = Parse(
      "CREATE TABLE stories(id int PRIMARY KEY, story_name VARCHAR(50), author "
      "VARCHAR(50), extra_col int, FOREIGN KEY (author) REFERENCES "
      "users(name));");
  std::cout << conn.ExecuteCreateTable(
                   *static_cast<sqlast::CreateTable *>(parsed.get()))
            << std::endl;
  dataflow::SchemaRef schema2 = dataflow::SchemaFactory::Create(
      *static_cast<sqlast::CreateTable *>(parsed.get()));
  parsed = Parse("CREATE INDEX idx ON stories (author);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
  parsed = Parse("CREATE INDEX idx ON stories (story_name);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
  parsed = Parse("CREATE INDEX idx ON stories (extra_col);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
  parsed = Parse("INSERT INTO users VALUES(1, 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO users (PII_id, name) VALUES(2, 'MALTE');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO users (name, PII_id) VALUES('ISHAN', 3);");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "3")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(1, 'K', 'KINAN', 1);");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(2, 'K', 'KINAN', 2);");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(3, 'K', 'MALTE', 3);");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(4, 'Kk', 'MALTE', 4);");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("SELECT * FROM stories WHERE story_name = 'K';");
  auto resultset = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset.Vec(), schema2);
  pelton::dataflow::Record record1{schema2};
  int64_t v0 = 1;
  std::unique_ptr<std::string> ptr1 = std::make_unique<std::string>("K");
  std::string *v1 = ptr1.get();  // Does not release ownership.
  std::unique_ptr<std::string> ptr2 = std::make_unique<std::string>("KINAN");
  std::string *v2 = ptr2.get();  // Does not release ownership.
  record1.SetData(v0, std::move(ptr1), std::move(ptr2), v0);
  pelton::dataflow::Record record2{schema2};
  v0 = 2;
  ptr1 = std::make_unique<std::string>("K");
  v1 = ptr1.get();  // Does not release ownership.
  ptr2 = std::make_unique<std::string>("KINAN");
  v2 = ptr2.get();  // Does not release ownership.
  record2.SetData(v0, std::move(ptr1), std::move(ptr2), v0);
  pelton::dataflow::Record record3{schema2};
  v0 = 3;
  ptr1 = std::make_unique<std::string>("K");
  v1 = ptr1.get();  // Does not release ownership.
  ptr2 = std::make_unique<std::string>("MALTE");
  v2 = ptr2.get();  // Does not release ownership.
  record3.SetData(v0, std::move(ptr1), std::move(ptr2), v0);
  std::vector<dataflow::Record *> ans = {&record1, &record2, &record3};
  CheckEqual(&resultset.rows(), ans);
  // assert(resultset)
  // ADD ASSERT STATEMENTS FOR REMAINING TESTS
  conn.Close();
}
TEST(RocksdbConnectionTest, MultipleValues) {
  DropDatabase();

  RocksdbConnection conn;
  conn.Open(DB_NAME);
  // Create table.
  auto parsed =
      Parse("CREATE TABLE users(PII_id int PRIMARY KEY, name VARCHAR(50));");
  std::cout << conn.ExecuteCreateTable(
                   *static_cast<sqlast::CreateTable *>(parsed.get()))
            << std::endl;
  dataflow::SchemaRef schema1 = dataflow::SchemaFactory::Create(
      *static_cast<sqlast::CreateTable *>(parsed.get()));
  parsed = Parse(
      "CREATE TABLE stories(id int PRIMARY KEY, story_name VARCHAR(50), author "
      "VARCHAR(50), FOREIGN KEY (author) REFERENCES users(name));");
  std::cout << conn.ExecuteCreateTable(
                   *static_cast<sqlast::CreateTable *>(parsed.get()))
            << std::endl;
  dataflow::SchemaRef schema2 = dataflow::SchemaFactory::Create(
      *static_cast<sqlast::CreateTable *>(parsed.get()));
  parsed = Parse("CREATE INDEX idx ON stories (story_name);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
  parsed = Parse("INSERT INTO users VALUES(1, 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO users (PII_id, name) VALUES(2, 'MALTE');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO users (name, PII_id) VALUES('ISHAN', 3);");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "3")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(1, 'K', 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(2, 'K', 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(3, 'K', 'MALTE');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(4, 'Kk', 'MALTE');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("SELECT * FROM stories WHERE story_name in ('K', 'Kk');");
  auto resultset = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  //   parsed = Parse("DELETE FROM stories WHERE story_name = 'Kk';");
  //   std::cout << conn.ExecuteDelete(*static_cast<sqlast::Delete
  //   *>(parsed.get()), "4") << std::endl;
  Print(resultset.Vec(), schema2);

  parsed = Parse("SELECT * FROM stories WHERE story_name in ('K', 'Kk');");
  auto newset = conn.ExecuteSelect(*static_cast<sqlast::Select *>(parsed.get()),
                                   schema2, {});
  Print(newset.Vec(), schema2);

  pelton::dataflow::Record record1{schema2};
  int64_t v0 = 1;
  std::unique_ptr<std::string> ptr1 = std::make_unique<std::string>("K");
  std::string *v1 = ptr1.get();  // Does not release ownership.
  std::unique_ptr<std::string> ptr2 = std::make_unique<std::string>("KINAN");
  std::string *v2 = ptr2.get();  // Does not release ownership.
  record1.SetData(v0, std::move(ptr1), std::move(ptr2));
  pelton::dataflow::Record record2{schema2};
  v0 = 2;
  ptr1 = std::make_unique<std::string>("K");
  v1 = ptr1.get();  // Does not release ownership.
  ptr2 = std::make_unique<std::string>("KINAN");
  v2 = ptr2.get();  // Does not release ownership.
  record2.SetData(v0, std::move(ptr1), std::move(ptr2));
  pelton::dataflow::Record record3{schema2};
  v0 = 3;
  ptr1 = std::make_unique<std::string>("K");
  v1 = ptr1.get();  // Does not release ownership.
  ptr2 = std::make_unique<std::string>("MALTE");
  v2 = ptr2.get();  // Does not release ownership.
  record3.SetData(v0, std::move(ptr1), std::move(ptr2));
  pelton::dataflow::Record record4{schema2};
  v0 = 4;
  ptr1 = std::make_unique<std::string>("Kk");
  v1 = ptr1.get();  // Does not release ownership.
  ptr2 = std::make_unique<std::string>("MALTE");
  v2 = ptr2.get();  // Does not release ownership.
  record4.SetData(v0, std::move(ptr1), std::move(ptr2));
  std::vector<dataflow::Record *> ans = {&record1, &record2, &record3,
                                         &record4};
  CheckEqual(&resultset.rows(), ans);
  // assert(resultset)
  // ADD ASSERT STATEMENTS FOR REMAINING TESTS
  conn.Close();
}
// TEST(RocksdbConnectionTest, ProjectionsAndRearrangements) {
//   DropDatabase();

//   RocksdbConnection conn(DB_NAME);
//   // Create table.
//   auto parsed =
//       Parse("CREATE TABLE users(PII_id int PRIMARY KEY, name VARCHAR(50));");
//   std::cout << conn.ExecuteStatement(parsed.get(), "") << std::endl;
//   dataflow::SchemaRef schema1 = dataflow::SchemaFactory::Create(
//       *static_cast<sqlast::CreateTable *>(parsed.get()));
//    parsed =
//        Parse("CREATE TABLE stories(id int PRIMARY KEY, story_name
//        VARCHAR(50), author VARCHAR(50), FOREIGN KEY (author) REFERENCES
//        users(name));");
//    std::cout << conn.ExecuteStatement(parsed.get(), "") << std::endl;
//    dataflow::SchemaRef schema2 = dataflow::SchemaFactory::Create(
//        *static_cast<sqlast::CreateTable *>(parsed.get()));
//   parsed =
//       Parse("CREATE INDEX idx ON stories (story_name);");
//   std::cout << conn.ExecuteStatement(parsed.get(), "") << std::endl;
//   parsed = Parse("INSERT INTO users VALUES(1, 'KINAN');");
//   std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert
//   *>(parsed.get()), "1").status << std::endl; parsed = Parse("INSERT INTO
//   users (PII_id, name) VALUES(2, 'MALTE');"); std::cout <<
//   conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
//   "2").status << std::endl; parsed = Parse("INSERT INTO users (name, PII_id)
//   VALUES('ISHAN', 3);"); std::cout <<
//   conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
//   "3").status << std::endl; parsed = Parse("INSERT INTO stories VALUES(1,
//   'K', 'KINAN');"); std::cout <<
//   conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
//   "1").status << std::endl; parsed = Parse("INSERT INTO stories VALUES(2,
//   'K', 'KINAN');"); std::cout <<
//   conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
//   "1").status << std::endl; parsed = Parse("INSERT INTO stories VALUES(3,
//   'K', 'MALTE');"); std::cout <<
//   conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
//   "2").status << std::endl; parsed = Parse("INSERT INTO stories VALUES(4,
//   'Kk', 'MALTE');"); std::cout <<
//   conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
//   "2").status << std::endl; parsed = Parse("SELECT id, author FROM stories
//   WHERE story_name = 'K';"); auto resultset1 =
//   conn.ExecuteSelectShard(parsed.get(), schema2, {}, "1");
//   Print(resultset1.Vec(), schema2);
//   //create new schema for projections
//   std::vector<std::string> names = {"id", "author"};
//   std::vector<CType> types = {CType::INT, CType::TEXT};
//   std::vector<ColumnID> keys = {0};
//   dataflow::SchemaRef out_schema = dataflow::SchemaFactory::Create(names,
//   types, keys); pelton::dataflow::Record record1{out_schema}; int64_t v0 = 1;
//   std::unique_ptr<std::string> ptr2 = std::make_unique<std::string>("KINAN");
//   std::string *v2 = ptr2.get();  // Does not release ownership.
//   record1.SetData(v0, std::move(ptr2));
//   pelton::dataflow::Record record2{out_schema};
//   v0 = 2;
//   ptr2 = std::make_unique<std::string>("KINAN");
//   v2 = ptr2.get();  // Does not release ownership.
//   record2.SetData(v0, std::move(ptr2));
//   pelton::dataflow::Record record3{out_schema};
//   v0 = 3;
//   ptr2 = std::make_unique<std::string>("MALTE");
//   v2 = ptr2.get();  // Does not release ownership.
//   record3.SetData(v0, std::move(ptr2));
//   std::vector<dataflow::Record*> ans = {&record1, &record2, &record3};
//   CheckEqual(&resultset1.rows(), ans);
//   parsed = Parse("SELECT author, id, story_name FROM stories WHERE story_name
//   = 'K';"); auto resultset2 = conn.ExecuteSelect(*static_cast<sqlast::Select
//   *>(parsed.get()), schema2,
//   {}); Print(resultset2.Vec(), schema2); names = {"author", "id",
//   "story_name"}; types = {CType::TEXT, CType::INT, CType::TEXT}; keys = {1};
//   out_schema = dataflow::SchemaFactory::Create(names, types, keys);
//   pelton::dataflow::Record record21{out_schema};
//   v0 = 1;
//   std::unique_ptr<std::string> ptr1 = std::make_unique<std::string>("K");
//   std::string *v1 = ptr1.get();  // Does not release ownership.
//   ptr2 = std::make_unique<std::string>("KINAN");
//   v2 = ptr2.get();  // Does not release ownership.
//   record21.SetData(std::move(ptr2),v0, std::move(ptr1));
//   pelton::dataflow::Record record22{out_schema};
//   v0 = 2;
//   ptr1 = std::make_unique<std::string>("K");
//   v1 = ptr1.get();  // Does not release ownership.
//   ptr2 = std::make_unique<std::string>("KINAN");
//   v2 = ptr2.get();  // Does not release ownership.
//   record22.SetData(std::move(ptr2), v0, std::move(ptr1));
//   pelton::dataflow::Record record23{out_schema};
//   v0 = 3;
//   ptr1 = std::make_unique<std::string>("K");
//   v1 = ptr1.get();  // Does not release ownership.
//   ptr2 = std::make_unique<std::string>("MALTE");
//   v2 = ptr2.get();  // Does not release ownership.
//   record23.SetData(std::move(ptr2), v0, std::move(ptr1));
//   ans = {&record21, &record22, &record23};
//   //CheckEqual(&resultset2.rows(), ans);
//   //assert(resultset)
//   //ADD ASSERT STATEMENTS FOR REMAINING TESTS
//   conn.Close();
//   RocksdbConnection::CloseAll();
// }
TEST(RocksdbConnectionTest, IndexOnShardedTable) {
  DropDatabase();
  RocksdbConnection conn;
  conn.Open(DB_NAME);
  // Create table.
  auto parsed =
      Parse("CREATE TABLE users(PII_id int PRIMARY KEY, name VARCHAR(50));");
  std::cout << conn.ExecuteCreateTable(
                   *static_cast<sqlast::CreateTable *>(parsed.get()))
            << std::endl;
  dataflow::SchemaRef schema1 = dataflow::SchemaFactory::Create(
      *static_cast<sqlast::CreateTable *>(parsed.get()));
  parsed = Parse("CREATE INDEX idx ON users (name);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
  parsed = Parse("INSERT INTO users VALUES(1, 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO users (PII_id, name) VALUES(2, 'MALTE');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO users (name, PII_id) VALUES('KINAN', 3);");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "3")
                   .status
            << std::endl;
  parsed = Parse("SELECT * FROM users WHERE name = 'KINAN';");
  auto resultset = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema1, {});

  pelton::dataflow::Record record1{schema1};
  int64_t v0 = 1;
  std::unique_ptr<std::string> ptr1 = std::make_unique<std::string>("KINAN");
  std::string *v1 = ptr1.get();  // Does not release ownership.
  record1.SetData(v0, std::move(ptr1));
  pelton::dataflow::Record record2{schema1};
  v0 = 3;
  ptr1 = std::make_unique<std::string>("KINAN");
  v1 = ptr1.get();  // Does not release ownership.
  record2.SetData(v0, std::move(ptr1));

  std::vector<dataflow::Record *> ans = {&record1, &record2};
  CheckEqual(&resultset.rows(), ans);

  Print(resultset.Vec(), schema1);
  conn.Close();
}

TEST(RocksdbConnectionTest, SelectonShardColumn) {
  DropDatabase();

  RocksdbConnection conn;
  conn.Open(DB_NAME);
  // Create table.
  auto parsed =
      Parse("CREATE TABLE users(PII_id int PRIMARY KEY, name VARCHAR(50));");
  std::cout << conn.ExecuteCreateTable(
                   *static_cast<sqlast::CreateTable *>(parsed.get()))
            << std::endl;
  dataflow::SchemaRef schema1 = dataflow::SchemaFactory::Create(
      *static_cast<sqlast::CreateTable *>(parsed.get()));
  parsed = Parse(
      "CREATE TABLE stories(id int , story_name VARCHAR(50), author "
      "VARCHAR(50) PRIMARY KEY, FOREIGN KEY (id) REFERENCES users(id));");
  std::cout << conn.ExecuteCreateTable(
                   *static_cast<sqlast::CreateTable *>(parsed.get()))
            << std::endl;
  dataflow::SchemaRef schema2 = dataflow::SchemaFactory::Create(
      *static_cast<sqlast::CreateTable *>(parsed.get()));
  parsed = Parse("CREATE INDEX idx ON stories (id);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
  parsed = Parse("INSERT INTO users VALUES(1, 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO users (PII_id, name) VALUES(2, 'MALTE');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO users (name, PII_id) VALUES('ISHAN', 3);");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "3")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(1, 'K', 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(1, 'Kk', 'KINAN1');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(3, 'K', 'MALTE');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(4, 'Kk', 'MALTE1');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  //   parsed = Parse("SELECT * FROM stories");
  //   auto resultsett = conn.ExecuteSelect(*static_cast<sqlast::Select
  //   *>(parsed.get()), schema2, {}); std::cout << "HERE SIZE: " <<
  //   resultsett.rows().size() << std::endl; for (int i = 0; i <
  //   resultsett.rows().size(); i++) {
  //       std::cout << "HERE: " << resultsett.rows()[i] << std::endl;
  //   }
  parsed = Parse("SELECT * FROM stories WHERE id = 1;");
  auto resultset = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  pelton::dataflow::Record record1{schema2};
  int64_t v0 = 1;
  std::unique_ptr<std::string> ptr1 = std::make_unique<std::string>("K");
  std::string *v1 = ptr1.get();  // Does not release ownership.
  std::unique_ptr<std::string> ptr2 = std::make_unique<std::string>("KINAN");
  std::string *v2 = ptr2.get();  // Does not release ownership.
  record1.SetData(v0, std::move(ptr1), std::move(ptr2));
  pelton::dataflow::Record record2{schema2};
  v0 = 1;
  ptr1 = std::make_unique<std::string>("Kk");
  v1 = ptr1.get();  // Does not release ownership.
  ptr2 = std::make_unique<std::string>("KINAN1");
  v2 = ptr2.get();  // Does not release ownership.
  record2.SetData(v0, std::move(ptr1), std::move(ptr2));
  std::vector<dataflow::Record *> ans = {&record1, &record2};
  CheckEqual(&resultset.rows(), ans);
  conn.Close();
}
TEST(RocksdbConnectionTest, SelectonShardColumnfromPK) {
  DropDatabase();

  RocksdbConnection conn;
  conn.Open(DB_NAME);
  // Create table.
  auto parsed =
      Parse("CREATE TABLE users(PII_id int PRIMARY KEY, name VARCHAR(50));");
  std::cout << conn.ExecuteCreateTable(
                   *static_cast<sqlast::CreateTable *>(parsed.get()))
            << std::endl;
  dataflow::SchemaRef schema1 = dataflow::SchemaFactory::Create(
      *static_cast<sqlast::CreateTable *>(parsed.get()));
  parsed = Parse(
      "CREATE TABLE stories(id int PRIMARY KEY, story_name VARCHAR(50), author "
      "VARCHAR(50), FOREIGN KEY (id) REFERENCES users(id));");
  std::cout << conn.ExecuteCreateTable(
                   *static_cast<sqlast::CreateTable *>(parsed.get()))
            << std::endl;
  dataflow::SchemaRef schema2 = dataflow::SchemaFactory::Create(
      *static_cast<sqlast::CreateTable *>(parsed.get()));
  parsed = Parse("CREATE INDEX idx ON stories (id);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
  parsed = Parse("INSERT INTO users VALUES(1, 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO users (PII_id, name) VALUES(2, 'MALTE');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO users (name, PII_id) VALUES('ISHAN', 3);");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "3")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(1, 'K', 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(2, 'K', 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(3, 'K', 'MALTE');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("INSERT INTO stories VALUES(4, 'Kk', 'MALTE');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("SELECT * FROM stories WHERE id = 1;");
  auto resultset = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  pelton::dataflow::Record record{schema2};
  int64_t v0 = 1;
  std::unique_ptr<std::string> ptr1 = std::make_unique<std::string>("K");
  std::string *v1 = ptr1.get();  // Does not release ownership.
  std::unique_ptr<std::string> ptr2 = std::make_unique<std::string>("KINAN");
  std::string *v2 = ptr2.get();  // Does not release ownership.
  record.SetData(v0, std::move(ptr1), std::move(ptr2));
  std::vector<dataflow::Record *> ans = {&record};
  CheckEqual(&resultset.rows(), ans);
  conn.Close();
}

}  // namespace
}  // namespace sql
}  // namespace pelton
