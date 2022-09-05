#include "pelton/sql/rocksdb/connection.h"

#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

#define DB_NAME "test"
#define DB_PATH "/tmp/pelton/rocksdb/test"
#define STR(s) std::make_unique<std::string>(s)

namespace pelton {
namespace sql {

namespace {

using CType = sqlast::ColumnDefinition::Type;
using Constraint = sqlast::ColumnConstraint::Type;
using EType = sqlast::Expression::Type;

std::unique_ptr<RocksdbConnection> InitializeDatabase() {
  // Drop the database (if it exists).
  std::filesystem::remove_all(DB_PATH);
  // Initialize a new connection.
  std::unique_ptr<RocksdbConnection> conn =
      std::make_unique<RocksdbConnection>();
  conn->Open(DB_NAME);
  return conn;
}

void Print(std::vector<dataflow::Record> &&records,
           const dataflow::SchemaRef &schema) {
  std::cout << schema << std::endl;
  for (auto &r : records) {
    std::cout << r << std::endl;
  }
  std::cout << std::endl;
}

void CheckEqual(const std::vector<dataflow::Record> &a,
                const std::vector<dataflow::Record> &b) {
  assert(a.size() == b.size());
  for (size_t i = 0; i < b.size(); i++) {
    assert(std::count(a.begin(), a.end(), b.at(i)) == 1);
  }
}

}  // namespace

TEST(RocksdbConnectionTest, CreateInsertSelect) {
  std::unique_ptr<RocksdbConnection> conn = InitializeDatabase();

  // Create Table.
  sqlast::CreateTable tbl("test_table");
  tbl.AddColumn("ID", sqlast::ColumnDefinition("ID", CType::INT));
  tbl.AddColumn("name", sqlast::ColumnDefinition("name", CType::TEXT));
  tbl.AddColumn("age", sqlast::ColumnDefinition("age", CType::INT));
  tbl.MutableColumn("ID").AddConstraint(
      sqlast::ColumnConstraint(Constraint::PRIMARY_KEY));
  dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(tbl);
  EXPECT_TRUE(conn->ExecuteCreateTable(tbl));

  // Insert into table.
  sqlast::Insert insert("test_table", false);
  insert.SetValues({"0", "'user1'", "20"});
  EXPECT_EQ(conn->ExecuteInsert(insert, "user1"), 1);

  insert.SetValues({"1", "'user2'", "25"});
  EXPECT_EQ(conn->ExecuteInsert(insert, "user2"), 1);

  insert.SetValues({"2", "'user3'", "30"});
  EXPECT_EQ(conn->ExecuteInsert(insert, "user3"), 1);

  insert.SetValues({"3", "'user4'", "35"});
  EXPECT_EQ(conn->ExecuteInsert(insert, "user4"), 1);

  std::vector<dataflow::Record> expected;
  expected.emplace_back(schema, true, 0_s, STR("user1"), 20_s);
  expected.emplace_back(schema, true, 1_s, STR("user2"), 25_s);
  expected.emplace_back(schema, true, 2_s, STR("user3"), 30_s);
  expected.emplace_back(schema, true, 3_s, STR("user4"), 35_s);

  // Select from table.
  sqlast::Select select("test_table");
  select.AddColumn("*");
  SqlResultSet result = conn->ExecuteSelect(select);
  CheckEqual(result.Vec(), expected);

  /*
  select.SetWhereClause(std::make_unique<sqlast::BinaryExpression>(EType::EQ));
  sqlast::BinaryExpression *where = select.GetWhereClause();
  where->SetLeft(std::make_unique<sqlast::ColumnExpression>("ID"));
  where->SetRight(std::make_unique<sqlast::LiteralExpression>("0"));
  where->SetRight(std::make_unique<sqlast::LiteralExpression>("1"));
  where->SetRight(std::make_unique<sqlast::LiteralExpression>("1"));
  where->SetRight(std::make_unique<sqlast::LiteralExpression>("2"));
  where->SetRight(std::make_unique<sqlast::LiteralExpression>("3"));
  where->SetRight(std::make_unique<sqlast::LiteralExpression>("4"));
  */
}

/*
TEST(RocksdbConnectionTest, UpdateWithViews) {
  // Create Views
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
  parsed = Parse("CREATE INDEX idx ON stories (author);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
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
  parsed = Parse("INSERT INTO stories VALUES(4, 'K', 'MALTE');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse(
      "CREATE VIEW v1 AS '\"SELECT * FROM stories WHERE stories.id != 1\"';");
  parsed = Parse("UPDATE stories SET story_name = 'new name' WHERE id = 4;");
  // Updates a value in a view
  conn.ExecuteUpdate(*static_cast<sqlast::Update *>(parsed.get()));
  parsed = Parse("SELECT * FROM stories WHERE story_name = 'new name';");
  auto resultset1 = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset1.Vec(), schema2);

  pelton::dataflow::Record record0{schema2};
  int64_t v0 = 4;
  std::unique_ptr<std::string> ptr1 = std::make_unique<std::string>("new name");
  std::string *v1 = ptr1.get();  // Does not release ownership.
  std::unique_ptr<std::string> ptr2 = std::make_unique<std::string>("MALTE");
  std::string *v2 = ptr2.get();  // Does not release ownership.
  record0.SetData(v0, std::move(ptr1), std::move(ptr2));

  std::vector<dataflow::Record *> ans = {&record0};
  CheckEqual(&resultset1.rows(), ans);

  parsed = Parse("DELETE FROM stories WHERE id = 4");
  conn.ExecuteDelete(*static_cast<sqlast::Delete *>(parsed.get()));
  parsed = Parse("SELECT * FROM stories WHERE author = 'MALTE';");
  auto resultset2 = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset2.Vec(), schema2);

  pelton::dataflow::Record record3{schema2};
  v0 = 3;
  ptr1 = std::make_unique<std::string>("K");
  v1 = ptr1.get();  // Does not release ownership.
  ptr2 = std::make_unique<std::string>("MALTE");
  v2 = ptr2.get();  // Does not release ownership.
  record3.SetData(v0, std::move(ptr1), std::move(ptr2));

  ans = {&record3};
  CheckEqual(&resultset2.rows(), ans);
  // assert(std::count(resultset.rows().begin(), resultset.rows().end(),
  // record1)); ADD ASSERT STATEMENTS FOR REMAINING TESTS
  conn.Close();
}

// TEST(RocksdbConnectionTest, ReturningTests) {
//   DropDatabase();
//   RocksdbConnection conn;
//   conn.Open(DB_NAME);
//   // Create table.
//   auto parsed =
//       Parse("CREATE TABLE users(PII_id int PRIMARY KEY, name VARCHAR(50));");
//   std::cout << conn.ExecuteCreateTable(
//                    *static_cast<sqlast::CreateTable *>(parsed.get()))
//             << std::endl;
//   dataflow::SchemaRef schema1 = dataflow::SchemaFactory::Create(
//       *static_cast<sqlast::CreateTable *>(parsed.get()));
//   parsed = Parse(
//       "CREATE TABLE stories(id int PRIMARY KEY, story_name VARCHAR(50),
//       author " " VARCHAR(50), FOREIGN KEY(author) REFERENCES users(name));");
//   std::cout << conn.ExecuteCreateTable(
//                    *static_cast<sqlast::CreateTable *>(parsed.get()))
//             << std::endl;
//   dataflow::SchemaRef schema2 = dataflow::SchemaFactory::Create(
//       *static_cast<sqlast::CreateTable *>(parsed.get()));
//   parsed = Parse("CREATE INDEX idx ON stories (story_name);");
//   std::cout << conn.ExecuteCreateIndex(
//                    *static_cast<sqlast::CreateIndex *>(parsed.get()))
//             << std::endl;
//   parsed = Parse("INSERT INTO users VALUES(1, 'KINAN');");
//   std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert
//   *>(parsed.get()),
//                                   "1")
//                    .status
//             << std::endl;
//   parsed = Parse("INSERT INTO users (PII_id, name) VALUES(2, 'MALTE');");
//   std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert
//   *>(parsed.get()),
//                                   "2")
//                    .status
//             << std::endl;
//   parsed = Parse("INSERT INTO users (name, PII_id) VALUES('ISHAN', 3);");
//   std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert
//   *>(parsed.get()),
//                                   "3")
//                    .status
//             << std::endl;
//   parsed = Parse("INSERT INTO stories VALUES(1, 'K', 'KINAN');");
//   std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert
//   *>(parsed.get()),
//                                   "1")
//                    .status
//             << std::endl;
//   parsed = Parse("INSERT INTO stories VALUES(2, 'K', 'KINAN');");
//   std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert
//   *>(parsed.get()),
//                                   "1")
//                    .status
//             << std::endl;
//   parsed = Parse("INSERT INTO stories VALUES(3, 'K', 'MALTE');");
//   std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert
//   *>(parsed.get()),
//                                   "2")
//                    .status
//             << std::endl;
//   parsed = Parse("INSERT INTO stories VALUES(4, 'Kk', 'KINAN');");
//   std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert
//   *>(parsed.get()),
//                                   "2")
//                    .status
//             << std::endl;
//   parsed = Parse("DELETE FROM stories WHERE story_name = 'K' RETURNING *;");
//   auto deleteset =
//       conn.ExecuteDelete(*static_cast<sqlast::Delete *>(parsed.get()));
//   // Test if the delete returns old records

//   pelton::dataflow::Record record1{schema2};
//   int64_t v0 = 1;
//   std::unique_ptr<std::string> ptr1 = std::make_unique<std::string>("K");
//   std::string *v1 = ptr1.get();  // Does not release ownership.
//   std::unique_ptr<std::string> ptr2 = std::make_unique<std::string>("KINAN");
//   std::string *v2 = ptr2.get();  // Does not release ownership.
//   record1.SetData(v0, std::move(ptr1), std::move(ptr2));

//   pelton::dataflow::Record record2{schema2};
//   v0 = 2;
//   ptr1 = std::make_unique<std::string>("K");
//   v1 = ptr1.get();  // Does not release ownership.
//   ptr2 = std::make_unique<std::string>("KINAN");
//   v2 = ptr2.get();  // Does not release ownership.
//   record2.SetData(v0, std::move(ptr1), std::move(ptr2));

//   pelton::dataflow::Record record3{schema2};
//   v0 = 3;
//   ptr1 = std::make_unique<std::string>("K");
//   v1 = ptr1.get();  // Does not release ownership.
//   ptr2 = std::make_unique<std::string>("MALTE");
//   v2 = ptr2.get();  // Does not release ownership.
//   record3.SetData(v0, std::move(ptr1), std::move(ptr2));

//   std::vector<dataflow::Record *> del = {&record1, &record2, &record3};
//   CheckEqual(&deleteset.ResultSets().begin()->rows(), del);

//   parsed = Parse("SELECT * FROM stories WHERE story_name = 'K';");
//   auto resultset = conn.ExecuteSelect(
//       *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
//   Print(resultset.Vec(), schema2);
//   std::vector<dataflow::Record *> ans = {};
//   CheckEqual(&resultset.rows(), ans);
//   parsed = Parse("INSERT INTO stories VALUES(1, 'Kk', 'MALTE');");
//   std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert
//   *>(parsed.get()),
//                                   "2")
//                    .status
//             << std::endl;
//   parsed = Parse("INSERT INTO stories VALUES(2, 'Kk', 'MALTE');");
//   std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert
//   *>(parsed.get()),
//                                   "2")
//                    .status
//             << std::endl;
//   parsed = Parse("INSERT INTO stories VALUES(3, 'k', 'KINAN');");
//   std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert
//   *>(parsed.get()),
//                                   "2")
//                    .status
//             << std::endl;
//   // parsed = Parse("UPDATE stories SET story_name = 'returned' WHERE id = 3
//   // RETURNING *"); auto updateset =
//   // conn.ExecuteUpdate(*static_cast<sqlast::Update *>(parsed.get()));
//   // // Test if the update returns old records

//   //  pelton::dataflow::Record record4{schema2};
//   //  v0 = 3;
//   //  ptr1 = std::make_unique<std::string>("k");
//   //  v1 = ptr1.get(); // Does not release ownership.
//   //  ptr2 = std::make_unique<std::string>("KINAN");
//   //  v2 = ptr2.get(); // Does not release ownership.
//   //  record4.SetData(v0, std::move(ptr1), std::move(ptr2));

//   //  std::vector<dataflow::Record *> upd = {&record4};
//   //  CheckEqual(&updateset.rows(), upd);

//   // parsed = Parse("SELECT * FROM stories WHERE story_name = 'returned';");
//   // auto resultset2 = conn.ExecuteSelect(
//   //     *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
//   // Print(resultset2.Vec(), schema2);

//   //  pelton::dataflow::Record record5{schema2};
//   //  v0 = 3;
//   //  ptr1 = std::make_unique<std::string>("returned");
//   //  v1 = ptr1.get(); // Does not release ownership.
//   //  ptr2 = std::make_unique<std::string>("KINAN");
//   //  v2 = ptr2.get(); // Does not release ownership.
//   //  record5.SetData(v0, std::move(ptr1), std::move(ptr2));

//   //  std::vector<dataflow::Record *> ans = {&record5};
//   //  CheckEqual(&resultset2.rows(), ans);
//   // assert(std::count(resultset.rows().begin(), resultset.rows().end(),
//   // record1)); ADD ASSERT STATEMENTS FOR REMAINING TESTS
//   conn.Close();
// }

TEST(RocksdbConnectionTest, PKBasedUpdateDelete) {
  // PK is searched, PK is updated, Delete using PK
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
  parsed = Parse("CREATE INDEX idx ON stories (author);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
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
  parsed = Parse("INSERT INTO stories VALUES(4, 'K', 'MALTE');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "2")
                   .status
            << std::endl;
  parsed = Parse("UPDATE stories SET story_name = 'new name' WHERE id = 4;");
  conn.ExecuteUpdate(*static_cast<sqlast::Update *>(parsed.get()));
  parsed = Parse("SELECT * FROM stories WHERE story_name = 'new name';");
  auto resultset1 = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset1.Vec(), schema2);

  pelton::dataflow::Record record0{schema2};
  int64_t v0 = 4;
  std::unique_ptr<std::string> ptr1 = std::make_unique<std::string>("new name");
  std::string *v1 = ptr1.get();  // Does not release ownership.
  std::unique_ptr<std::string> ptr2 = std::make_unique<std::string>("MALTE");
  std::string *v2 = ptr2.get();  // Does not release ownership.
  record0.SetData(v0, std::move(ptr1), std::move(ptr2));

  std::vector<dataflow::Record *> ans = {&record0};
  CheckEqual(&resultset1.rows(), ans);

  parsed = Parse("DELETE FROM stories WHERE id = 4");
  conn.ExecuteDelete(*static_cast<sqlast::Delete *>(parsed.get()));
  parsed = Parse("SELECT * FROM stories WHERE author = 'MALTE';");
  auto resultset2 = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset2.Vec(), schema2);

  pelton::dataflow::Record record3{schema2};
  v0 = 3;
  ptr1 = std::make_unique<std::string>("K");
  v1 = ptr1.get();  // Does not release ownership.
  ptr2 = std::make_unique<std::string>("MALTE");
  v2 = ptr2.get();  // Does not release ownership.
  record3.SetData(v0, std::move(ptr1), std::move(ptr2));

  ans = {&record3};
  CheckEqual(&resultset2.rows(), ans);
  // assert(std::count(resultset.rows().begin(), resultset.rows().end(),
  // record1)); ADD ASSERT STATEMENTS FOR REMAINING TESTS
  conn.Close();
}

TEST(RocksdbConnectionTest, UpdateDeleteNone) {
  // Nothing is updated or deleted as it doesn't meet the criterions
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
  parsed = Parse("CREATE INDEX idx2 ON stories (id);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
  parsed = Parse(
      "UPDATE stories SET story_name = 'new name' WHERE story_name = 'K';");
  conn.ExecuteUpdate(*static_cast<sqlast::Update *>(parsed.get()));
  parsed = Parse("SELECT * FROM stories WHERE story_name = 'new name';");
  auto resultset1 = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset1.Vec(), schema2);
  CheckEqual(&resultset1.rows(), {});

  parsed = Parse("DELETE FROM stories WHERE story_name = 'new name';");
  conn.ExecuteDelete(*static_cast<sqlast::Delete *>(parsed.get()));
  parsed = Parse(
      "SELECT * FROM stories WHERE story_name in ('new name', 'K', 'rand');");
  auto resultset2 = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset2.Vec(), schema2);
  CheckEqual(&resultset2.rows(), {});

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
  parsed = Parse("UPDATE stories SET story_name = 'new name' WHERE id = 5;");
  conn.ExecuteUpdate(*static_cast<sqlast::Update *>(parsed.get()));
  parsed = Parse("SELECT * FROM stories WHERE story_name = 'new name';");
  auto resultset3 = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset3.Vec(), schema2);
  CheckEqual(&resultset3.rows(), {});

  parsed = Parse("DELETE FROM stories WHERE story_name = 'new name';");
  conn.ExecuteDelete(*static_cast<sqlast::Delete *>(parsed.get()));
  parsed = Parse("SELECT * FROM stories WHERE id in (1, 2, 3, 4, 5);");
  auto resultset4 = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset4.Vec(), schema2);

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
  CheckEqual(&resultset4.rows(), ans);
  // assert(std::count(resultset.rows().begin(), resultset.rows().end(),
  // record1)); ADD ASSERT STATEMENTS FOR REMAINING TESTS
  conn.Close();
}

TEST(RocksdbConnectionTest, UpdateDeleteAll) {
  // Everything is updated and then deleted
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
  parsed = Parse("CREATE INDEX idx2 ON stories (id);");
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
  parsed = Parse(
      "UPDATE stories SET story_name = 'new name' WHERE story_name = 'K';");
  conn.ExecuteUpdate(*static_cast<sqlast::Update *>(parsed.get()));
  parsed = Parse("SELECT * FROM stories WHERE story_name = 'new name';");
  auto resultset1 = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset1.Vec(), schema2);

  pelton::dataflow::Record record1{schema2};
  int64_t v0 = 1;
  std::unique_ptr<std::string> ptr1 = std::make_unique<std::string>("new name");
  std::string *v1 = ptr1.get();  // Does not release ownership.
  std::unique_ptr<std::string> ptr2 = std::make_unique<std::string>("KINAN");
  std::string *v2 = ptr2.get();  // Does not release ownership.
  record1.SetData(v0, std::move(ptr1), std::move(ptr2));

  pelton::dataflow::Record record2{schema2};
  v0 = 2;
  ptr1 = std::make_unique<std::string>("new name");
  v1 = ptr1.get();  // Does not release ownership.
  ptr2 = std::make_unique<std::string>("KINAN");
  v2 = ptr2.get();  // Does not release ownership.
  record2.SetData(v0, std::move(ptr1), std::move(ptr2));

  pelton::dataflow::Record record3{schema2};
  v0 = 3;
  ptr1 = std::make_unique<std::string>("new name");
  v1 = ptr1.get();  // Does not release ownership.
  ptr2 = std::make_unique<std::string>("MALTE");
  v2 = ptr2.get();  // Does not release ownership.
  record3.SetData(v0, std::move(ptr1), std::move(ptr2));

  std::vector<dataflow::Record *> ans = {&record1, &record2, &record3};
  CheckEqual(&resultset1.rows(), ans);

  parsed = Parse(
      "UPDATE stories SET story_name = 'new name' WHERE story_name = 'Kk';");
  conn.ExecuteUpdate(*static_cast<sqlast::Update *>(parsed.get()));

  parsed = Parse("DELETE FROM stories WHERE story_name = 'new name';");
  conn.ExecuteDelete(*static_cast<sqlast::Delete *>(parsed.get()));
  parsed = Parse("SELECT * FROM stories WHERE story_name = 'new name';");
  auto resultset2 = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset2.Vec(), schema2);
  CheckEqual(&resultset2.rows(), {});  // All records deleted
  // assert(std::count(resultset.rows().begin(), resultset.rows().end(),
  // record1)); ADD ASSERT STATEMENTS FOR REMAINING TESTS
  conn.Close();
}

TEST(RocksdbConnectionTest, BASICDeleteTest) {
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
  parsed = Parse("DELETE FROM stories WHERE story_name = 'K';");
  conn.ExecuteDelete(*static_cast<sqlast::Delete *>(parsed.get()));
  parsed = Parse("SELECT * FROM stories WHERE story_name = 'K';");
  auto resultset = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset.Vec(), schema2);
  std::vector<dataflow::Record *> ans = {};
  CheckEqual(&resultset.rows(), ans);
  // assert(std::count(resultset.rows().begin(), resultset.rows().end(),
  // record1)); ADD ASSERT STATEMENTS FOR REMAINING TESTS
  conn.Close();
}

TEST(RocksdbConnectionTest, BASICUpdateTest) {
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
  parsed = Parse("CREATE INDEX idx2 ON stories (id);");
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
  parsed = Parse("UPDATE stories SET story_name = 'new name' WHERE id = 3");
  conn.ExecuteUpdate(*static_cast<sqlast::Update *>(parsed.get()));
  parsed = Parse("SELECT * FROM stories WHERE story_name = 'new name';");
  auto resultset = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  Print(resultset.Vec(), schema2);
  pelton::dataflow::Record record1{schema2};
  int64_t v0 = 3;
  std::unique_ptr<std::string> ptr1 = std::make_unique<std::string>("new name");
  std::string *v1 = ptr1.get();  // Does not release ownership.
  std::unique_ptr<std::string> ptr2 = std::make_unique<std::string>("MALTE");
  std::string *v2 = ptr2.get();  // Does not release ownership.
  record1.SetData(v0, std::move(ptr1), std::move(ptr2));
  pelton::dataflow::Record record2{schema2};

  std::vector<dataflow::Record *> ans = {&record1};
  for (int i = 0; i < resultset.rows().size(); i++) {
    std::cout << "printing record: " << resultset.rows()[i] << std::endl;
  }
  CheckEqual(&resultset.rows(), ans);
  // assert(std::count(resultset.rows().begin(), resultset.rows().end(),
  // record1)); ADD ASSERT STATEMENTS FOR REMAINING TESTS
  conn.Close();
}

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
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
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

TEST(RocksdbConnectionTest, ExecuteReplace) {
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
  parsed = Parse("CREATE INDEX idx2 ON users (name);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
  parsed = Parse("CREATE INDEX idx3 ON users (PII_id);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
  parsed = Parse("CREATE INDEX idx4 ON stories (story_name);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
  parsed = Parse("CREATE INDEX idx5 ON stories (author);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
  parsed = Parse("CREATE INDEX idx6 ON stories (id);");
  std::cout << conn.ExecuteCreateIndex(
                   *static_cast<sqlast::CreateIndex *>(parsed.get()))
            << std::endl;
  parsed = Parse("INSERT INTO users VALUES(1, 'KINAN');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("REPLACE INTO users VALUES(1, 'VEDANT');");
  std::cout << conn.ExecuteReplace(*static_cast<sqlast::Insert *>(parsed.get()),
                                   "1")
                   .status
            << std::endl;
  parsed = Parse("SELECT * FROM users WHERE PII_id = 1;");
  auto resultset = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema1, {});
  parsed = Parse("SELECT * FROM users WHERE name = 'KINAN';");
  auto resultset2 = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema1, {});
  parsed = Parse("SELECT * FROM users WHERE name = 'VEDANT';");
  auto resultset3 = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema1, {});

  pelton::dataflow::Record record1{schema1};
  int64_t v0 = 1;
  std::unique_ptr<std::string> ptr1 = std::make_unique<std::string>("VEDANT");
  std::string *v1 = ptr1.get();  // Does not release ownership.

  record1.SetData(v0, std::move(ptr1));
  std::vector<dataflow::Record *> ans = {&record1};
  CheckEqual(&resultset.rows(), ans);
  CheckEqual(&resultset2.rows(), {});
  CheckEqual(&resultset3.rows(), ans);

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
  parsed = Parse("INSERT INTO stories VALUES(1, 'Kk', 'VEDANT');");
  std::cout << conn.ExecuteInsert(*static_cast<sqlast::Insert *>(parsed.get()),
                                  "1")
                   .status
            << std::endl;
  parsed = Parse("REPLACE INTO stories VALUES(2, 'K', 'MALTE');");
  std::cout << conn.ExecuteReplace(*static_cast<sqlast::Insert *>(parsed.get()),
                                   "2")
                   .status
            << std::endl;
  parsed = Parse("REPLACE INTO stories VALUES(2, 'new name', 'MALTE');");
  std::cout << conn.ExecuteReplace(*static_cast<sqlast::Insert *>(parsed.get()),
                                   "2")
                   .status
            << std::endl;
  parsed = Parse("SELECT * FROM stories WHERE id = 2;");
  resultset = conn.ExecuteSelect(*static_cast<sqlast::Select *>(parsed.get()),
                                 schema2, {});
  parsed = Parse("SELECT * FROM stories WHERE author = 'MALTE';");
  resultset2 = conn.ExecuteSelect(*static_cast<sqlast::Select *>(parsed.get()),
                                  schema2, {});
  parsed = Parse("SELECT * FROM stories WHERE story_name = 'K';");
  resultset3 = conn.ExecuteSelect(*static_cast<sqlast::Select *>(parsed.get()),
                                  schema2, {});
  v0 = 2;
  ptr1 = std::make_unique<std::string>("new name");
  v1 = ptr1.get();  // Does not release ownership.
  std::unique_ptr<std::string> ptr2 = std::make_unique<std::string>("MALTE");
  std::string *v2 = ptr2.get();  // Does not release ownership.
  pelton::dataflow::Record record2{schema2};
  record2.SetData(v0, std::move(ptr1), std::move(ptr2));
  ans = {&record2};
  CheckEqual(&resultset.rows(), ans);
  CheckEqual(&resultset2.rows(), ans);
  CheckEqual(&resultset3.rows(), {});

  // replacing into another shard
  parsed = Parse("REPLACE INTO stories VALUES(2, 'new name 2', 'VEDANT');");
  std::cout << conn.ExecuteReplace(*static_cast<sqlast::Insert *>(parsed.get()),
                                   "2")
                   .status
            << std::endl;
  parsed = Parse("SELECT * FROM stories WHERE id = 2;");
  resultset = conn.ExecuteSelect(*static_cast<sqlast::Select *>(parsed.get()),
                                 schema2, {});
  parsed = Parse(
      "SELECT * FROM stories WHERE author = 'VEDANT' AND story_name = 'new "
      "name 2';");
  resultset2 = conn.ExecuteSelect(*static_cast<sqlast::Select *>(parsed.get()),
                                  schema2, {});
  parsed = Parse("SELECT * FROM stories WHERE author = 'MALTE';");
  resultset3 = conn.ExecuteSelect(*static_cast<sqlast::Select *>(parsed.get()),
                                  schema2, {});
  parsed = Parse("SELECT * FROM stories WHERE story_name = 'new name';");
  auto resultset4 = conn.ExecuteSelect(
      *static_cast<sqlast::Select *>(parsed.get()), schema2, {});
  v0 = 2;
  ptr1 = std::make_unique<std::string>("new name 2");
  v1 = ptr1.get();  // Does not release ownership.
  ptr2 = std::make_unique<std::string>("VEDANT");
  v2 = ptr2.get();  // Does not release ownership.
  record2.SetData(v0, std::move(ptr1), std::move(ptr2));
  ans = {&record2};
  CheckEqual(&resultset.rows(), ans);
  CheckEqual(&resultset2.rows(), ans);
  CheckEqual(&resultset3.rows(), {});
  CheckEqual(&resultset4.rows(), {});
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
  parsed = Parse("INSERT INTO stories VALUES(3, 'K', 'MALTE');f");
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
*/

}  // namespace sql
}  // namespace pelton
