#include "pelton/sql/connections/rocksdb.h"

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

namespace pelton {
namespace sql {

namespace {

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
  RocksdbConnection conn("/tmp/rocksdb/test");
  // Create table.
  auto parsed =
      Parse("CREATE TABLE tbl(id int PRIMARY KEY, name VARCHAR(50));");
  std::cout << conn.ExecuteStatement(parsed.get()) << std::endl;
  dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(
      *static_cast<sqlast::CreateTable *>(parsed.get()));
  // Insert rows.
  parsed = Parse("INSERT INTO tbl VALUES(1, 'KINAN');");
  std::cout << conn.ExecuteUpdate(parsed.get()) << std::endl;
  parsed = Parse("INSERT INTO tbl (id, name) VALUES(2, 'MALTE');");
  std::cout << conn.ExecuteUpdate(parsed.get()) << std::endl;
  parsed = Parse("INSERT INTO tbl (name, id) VALUES('ISHAN', 3);");
  std::cout << conn.ExecuteUpdate(parsed.get()) << std::endl;
  parsed = Parse("INSERT INTO tbl (name, id) VALUES(NULL, 4);");
  std::cout << conn.ExecuteUpdate(parsed.get()) << std::endl;
  parsed = Parse("INSERT INTO tbl (id) VALUES(5);");
  std::cout << conn.ExecuteUpdate(parsed.get()) << std::endl;

  // These should fail.
  // parsed = Parse("INSERT INTO tbl (name) VALUES('JUSTUS');");
  // std::cout << conn.ExecuteUpdate(parsed.get()) << std::endl;
  // parsed = Parse("INSERT INTO tbl (name, id) VALUES('BENJI', NULL);");
  // std::cout << conn.ExecuteUpdate(parsed.get()) << std::endl;

  // Read rows.
  parsed = Parse("SELECT * FROM tbl;");
  auto records = conn.ExecuteQuery(parsed.get(), schema, {});
  Print(std::move(records), schema);
  parsed = Parse("SELECT * FROM tbl WHERE id > 2;");
  records = conn.ExecuteQuery(parsed.get(), schema, {});
  Print(std::move(records), schema);
  parsed = Parse("SELECT * FROM tbl WHERE name = 'KINAN';");
  records = conn.ExecuteQuery(parsed.get(), schema, {});
  Print(std::move(records), schema);
  parsed = Parse("SELECT * FROM tbl WHERE name = 'KINAN' AND id > 2;");
  records = conn.ExecuteQuery(parsed.get(), schema, {});
  Print(std::move(records), schema);
  parsed = Parse("SELECT * FROM tbl WHERE id = 2;");
  records = conn.ExecuteQuery(parsed.get(), schema, {});
  Print(std::move(records), schema);
  parsed = Parse("SELECT * FROM tbl WHERE name = 'KINAN' AND id = 2;");
  records = conn.ExecuteQuery(parsed.get(), schema, {});
  Print(std::move(records), schema);
  parsed = Parse("SELECT * FROM tbl WHERE name = 'MALTE' AND id = 2;");
  records = conn.ExecuteQuery(parsed.get(), schema, {});
  Print(std::move(records), schema);

  conn.Close();
  RocksdbConnection::CloseAll();
}

}  // namespace sql
}  // namespace pelton
