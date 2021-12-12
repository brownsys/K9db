#include "pelton/sql/connections/rocksdb_connection.h"

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
#define DB_PATH "/var/pelton/rocksdb/test"

namespace pelton {
namespace sql {

namespace {

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

}  // namespace

TEST(RocksdbConnectionTest, OpenTest) {
  DropDatabase();

  RocksdbConnection conn(DB_NAME);
  // Create table.
  auto parsed =
      Parse("CREATE TABLE tbl(id int PRIMARY KEY, name VARCHAR(50));");
  std::cout << conn.ExecuteStatement(parsed.get(), "") << std::endl;
  dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(
      *static_cast<sqlast::CreateTable *>(parsed.get()));
  // Insert rows.
  parsed = Parse("INSERT INTO tbl VALUES(1, 'KINAN');");
  std::cout << conn.ExecuteUpdate(parsed.get(), "") << std::endl;
  parsed = Parse("INSERT INTO tbl (id, name) VALUES(2, 'MALTE');");
  std::cout << conn.ExecuteUpdate(parsed.get(), "") << std::endl;
  parsed = Parse("INSERT INTO tbl (name, id) VALUES('ISHAN', 3);");
  std::cout << conn.ExecuteUpdate(parsed.get(), "") << std::endl;
  parsed = Parse("INSERT INTO tbl (name, id) VALUES(NULL, 4);");
  std::cout << conn.ExecuteUpdate(parsed.get(), "") << std::endl;
  parsed = Parse("INSERT INTO tbl (id) VALUES(5);");
  std::cout << conn.ExecuteUpdate(parsed.get(), "") << std::endl;

  // These should fail.
  // parsed = Parse("INSERT INTO tbl (name) VALUES('JUSTUS');");
  // std::cout << conn.ExecuteUpdate(parsed.get(), "") << std::endl;
  // parsed = Parse("INSERT INTO tbl (name, id) VALUES('BENJI', NULL);");
  // std::cout << conn.ExecuteUpdate(parsed.get(), "") << std::endl;

  // Read rows.
  parsed = Parse("SELECT * FROM tbl;");
  auto resultset = conn.ExecuteQuery(parsed.get(), schema, {}, "");
  Print(resultset.Vec(), schema);
  parsed = Parse("SELECT * FROM tbl WHERE id > 2;");
  resultset = conn.ExecuteQuery(parsed.get(), schema, {}, "");
  Print(resultset.Vec(), schema);
  parsed = Parse("SELECT * FROM tbl WHERE name = 'KINAN';");
  resultset = conn.ExecuteQuery(parsed.get(), schema, {}, "");
  Print(resultset.Vec(), schema);
  parsed = Parse("SELECT * FROM tbl WHERE name = 'KINAN' AND id > 2;");
  resultset = conn.ExecuteQuery(parsed.get(), schema, {}, "");
  Print(resultset.Vec(), schema);
  parsed = Parse("SELECT * FROM tbl WHERE id = 2;");
  resultset = conn.ExecuteQuery(parsed.get(), schema, {}, "");
  Print(resultset.Vec(), schema);
  parsed = Parse("SELECT * FROM tbl WHERE name = 'KINAN' AND id = 2;");
  resultset = conn.ExecuteQuery(parsed.get(), schema, {}, "");
  Print(resultset.Vec(), schema);
  parsed = Parse("SELECT * FROM tbl WHERE name = 'MALTE' AND id = 2;");
  resultset = conn.ExecuteQuery(parsed.get(), schema, {}, "");
  Print(resultset.Vec(), schema);

  // Replace.
  parsed = Parse("REPLACE INTO tbl VALUES(5, 'BENJI');");
  resultset = conn.ExecuteQuery(parsed.get(), schema, {}, "");
  Print(resultset.Vec(), schema);
  parsed = Parse("REPLACE INTO tbl VALUES(3, 'ISHAN-NEW');");
  std::cout << conn.ExecuteUpdate(parsed.get(), "") << std::endl;

  // Read rows.
  parsed = Parse("SELECT * FROM tbl;");
  resultset = conn.ExecuteQuery(parsed.get(), schema, {}, "");
  Print(resultset.Vec(), schema);

  // Delete rows.
  parsed = Parse("DELETE FROM tbl WHERE name = 'BENJI';");
  resultset = conn.ExecuteQuery(parsed.get(), schema, {}, "");
  Print(resultset.Vec(), schema);
  parsed = Parse("DELETE FROM tbl WHERE id = 2;");
  std::cout << conn.ExecuteUpdate(parsed.get(), "") << std::endl;

  // Read rows.
  parsed = Parse("SELECT * FROM tbl;");
  resultset = conn.ExecuteQuery(parsed.get(), schema, {}, "");
  Print(resultset.Vec(), schema);

  // Update rows.
  parsed = Parse("UPDATE tbl SET id = 10 WHERE id = 3;");
  std::cout << conn.ExecuteUpdate(parsed.get(), "") << std::endl;
  parsed = Parse("UPDATE tbl SET name = NULL WHERE id = 2;");
  std::cout << conn.ExecuteUpdate(parsed.get(), "") << std::endl;
  parsed = Parse("UPDATE tbl SET name = 'DOESNOTAPPEAR' WHERE id = 33;");
  std::cout << conn.ExecuteUpdate(parsed.get(), "") << std::endl;

  // Read rows.
  parsed = Parse("SELECT * FROM tbl;");
  resultset = conn.ExecuteQuery(parsed.get(), schema, {}, "");
  Print(resultset.Vec(), schema);

  conn.Close();
  RocksdbConnection::CloseAll();
}

}  // namespace sql
}  // namespace pelton
