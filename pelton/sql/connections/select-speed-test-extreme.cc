#include <filesystem>
#include <chrono>  // NOLINT [build/c++11]
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>


#include "pelton/sql/connections/rocksdb_connection.h"
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

std::string find_shard(std::string query) {
  auto start = query.find("user");
  auto len = query.substr(start).find("'");
  return query.substr(start, len);
}

// Drop the database (if it exists).
void DropDatabase() { std::filesystem::remove_all(DB_PATH); }

std::unique_ptr<sqlast::AbstractStatement> Parse(const std::string &sql) {
  sqlast::SQLParser parser;
  // std::unique_ptr<sqlast::AbstractStatement> statement,
  auto statusor = parser.Parse(sql);
  EXPECT_TRUE(statusor.ok()) << statusor.status();
  return std::move(statusor.value());
}

TEST(RocksdbConnectionTest, SpeedTest) {
  DropDatabase();
  // create tables
  std::fstream s;
  s.open("pelton/sql/connections/pelton-schema-extreme.sql", std::ios::in);
  assert(s.is_open());
  RocksdbConnection conn(DB_NAME);
  std::string cmd;
  std::unique_ptr<sqlast::AbstractStatement> parsed;
  // dataflow::SchemaRef schema;
  int count = 0;
  dataflow::SchemaRef schema;
  while (getline(s, cmd)) {
    count += 1;
    parsed = Parse(cmd);
    conn.ExecuteStatement(parsed.get(), "");
    if (count == 2) {
      schema = dataflow::SchemaFactory::Create(
          *static_cast<sqlast::CreateTable *>(parsed.get()));
    }
  }
  s.close();
  // std::cout << "total inserts" << count << std::endl;
  // do insertions
  std::fstream i;
  i.open("pelton/sql/connections/pelton-insert-extreme.sql", std::ios::in);
  // dataflow::SchemaRef schema;
  while (getline(i, cmd)) {
    count += 1;
    parsed = Parse(cmd);
    conn.ExecuteUpdate(parsed.get(), find_shard(cmd));
  }
  i.close();
  // std::cout << "total inserts" << count << std::endl;
  // do selects
  std::fstream l;
  l.open("pelton/sql/connections/pelton-load-extreme.sql", std::ios::in);
  // dataflow::SchemaRef schema;
  std::vector<std::unique_ptr<sqlast::AbstractStatement>> selects;
  while (getline(l, cmd)) {
    count += 1;
    parsed = Parse(cmd);
    selects.push_back(std::move(parsed));
  }
  auto start = std::chrono::steady_clock::now();
  for (auto &i : selects) {
    conn.ExecuteQueryNoShard(i.get(), schema, {});
    // int j = 1 + 1;
  }
  auto end = std::chrono::steady_clock::now();
  auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                  .count();
  assert(count == 35003);
  std::cout << "total inserts" << count << std::endl;
  std::cout << "statements executed sucessfully!" << std::endl;
  std::cout << "total time taken be inserts: " << diff << " ms" << std::endl;
  l.close();
  conn.Close();
  RocksdbConnection::CloseAll();
}

}  // namespace
}  // namespace sql
}  // namespace pelton
