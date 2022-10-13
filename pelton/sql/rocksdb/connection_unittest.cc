#include "pelton/sql/rocksdb/connection.h"

#include <filesystem>
#include <iostream>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"
#include "pelton/util/shard_name.h"

namespace pelton {
namespace sql {

/*
 * Global configuration.
 */

#define DB_NAME "rocksdbconnection-test"
#define DB_PATH "/tmp/pelton/rocksdb/rocksdbconnection-test"
#define STR(s) std::make_unique<std::string>(s)

/*
 * Shorthand type aliases.
 */

using CType = sqlast::ColumnDefinition::Type;
using Constraint = sqlast::ColumnConstraint::Type;
using EType = sqlast::Expression::Type;
using BinExpr = sqlast::BinaryExpression;
using ColExpr = sqlast::ColumnExpression;
using LitExpr = sqlast::LiteralExpression;
using ListExpr = sqlast::LiteralListExpression;

/*
 * Helpers for retrieving expected records.
 */

namespace {

// The table schema.
dataflow::SchemaRef SCHEMA = dataflow::SchemaFactory::Create(
    {"ID", "name", "age"}, {CType::INT, CType::TEXT, CType::INT}, {0});

// Records in the table.
dataflow::Record __ARR[] = {{SCHEMA, false, 0_s, STR("user1"), 20_s},
                            {SCHEMA, false, 1_s, STR("user2"), 25_s},
                            {SCHEMA, false, 2_s, STR("user3"), 30_s},
                            {SCHEMA, false, 3_s, STR("user3"), 35_s}};

std::vector<dataflow::Record> RECORDS{
    std::make_move_iterator(std::begin(__ARR)),
    std::make_move_iterator(std::end(__ARR))};

std::vector<dataflow::Record> EMPTY;

// Get any subset of records.
std::vector<dataflow::Record> GetRecords(const std::vector<int> &ids) {
  std::vector<dataflow::Record> result;
  for (int i : ids) {
    result.push_back(RECORDS.at(i).Copy());
  }
  return result;
}

}  // namespace

/*
 * Helpers for initialize the database.
 */

namespace {

std::unique_ptr<RocksdbConnection> CONN;

// Database unit operations.
void InitializeDatabase() {
  // Drop the database (if it exists).
  std::filesystem::remove_all(DB_PATH);
  // Initialize a new connection.
  CONN = std::make_unique<RocksdbConnection>();
  CONN->Open(DB_NAME);
}

// Clear the database.
void CleanDatabase() {
  CONN->Close();
  CONN = nullptr;
  // Drop the database (if it exists).
  std::filesystem::remove_all(DB_PATH);
}

void CreateTable() {
  // Create Table.
  sqlast::CreateTable tbl("test_table");
  tbl.AddColumn("ID", sqlast::ColumnDefinition("ID", CType::INT));
  tbl.AddColumn("name", sqlast::ColumnDefinition("name", CType::TEXT));
  tbl.AddColumn("age", sqlast::ColumnDefinition("age", CType::INT));
  tbl.MutableColumn("ID").AddConstraint(
      sqlast::ColumnConstraint(Constraint::PRIMARY_KEY));
  EXPECT_TRUE(CONN->ExecuteCreateTable(tbl));
}

void CreateNameIndex() {
  // Create Table.
  sqlast::CreateIndex idx(true, "name_index", "test_table", "name");
  EXPECT_TRUE(CONN->ExecuteCreateIndex(idx));
}

void InsertData() {
  // Insert into table.
  sqlast::Insert insert("test_table", false);
  insert.SetValues({"0", "'user1'", "20"});
  EXPECT_EQ(CONN->ExecuteInsert(insert, util::ShardName("user", "1")), 1);

  insert.SetValues({"1", "'user2'", "25"});
  EXPECT_EQ(CONN->ExecuteInsert(insert, util::ShardName("user", "2")), 1);

  insert.SetValues({"2", "'user3'", "30"});
  EXPECT_EQ(CONN->ExecuteInsert(insert, util::ShardName("user", "3")), 1);

  insert.SetValues({"3", "'user3'", "35"});
  EXPECT_EQ(CONN->ExecuteInsert(insert, util::ShardName("user", "3")), 1);

  insert.SetValues({"3", "'user3'", "35"});
  EXPECT_EQ(CONN->ExecuteInsert(insert, util::ShardName("user", "2")), 1);
}

}  // namespace

/*
 * Helpers for select/delete with where clauses.
 */

namespace {

// Transform map -> where condition.
std::unique_ptr<BinExpr> MakeWhere(
    const std::unordered_map<std::string, std::vector<std::string>> &map) {
  // Transform map to condition.
  std::unique_ptr<BinExpr> condition(nullptr);
  for (const auto &[col, vals] : map) {
    // Transform current clause.
    std::unique_ptr<BinExpr> clause(nullptr);
    if (vals.size() > 1) {
      clause = std::make_unique<BinExpr>(EType::IN);
      clause->SetLeft(std::make_unique<ColExpr>(col));
      clause->SetRight(std::make_unique<ListExpr>(vals));
    } else if (vals.front() == "NULL") {
      clause = std::make_unique<BinExpr>(EType::IS);
      clause->SetLeft(std::make_unique<ColExpr>(col));
      clause->SetRight(std::make_unique<LitExpr>("NULL"));
    } else {
      clause = std::make_unique<BinExpr>(EType::EQ);
      clause->SetLeft(std::make_unique<ColExpr>(col));
      clause->SetRight(std::make_unique<LitExpr>(vals.front()));
    }

    // Conjunction.
    if (condition != nullptr) {
      std::unique_ptr<BinExpr> conj = std::make_unique<BinExpr>(EType::AND);
      conj->SetLeft(std::move(condition));
      conj->SetRight(std::move(clause));
      condition = std::move(conj);
    } else {
      condition = std::move(clause);
    }
  }
  return condition;
}

// Select/Delete by.
SqlResultSet SelectBy(
    const std::unordered_map<std::string, std::vector<std::string>> &map) {
  sqlast::Select select("test_table");
  select.AddColumn("*");
  select.SetWhereClause(MakeWhere(map));

  return CONN->ExecuteSelect(select);
}
SqlResultSet DeleteBy(
    const std::unordered_map<std::string, std::vector<std::string>> &map) {
  sqlast::Delete del("test_table");
  del.SetWhereClause(MakeWhere(map));

  return CONN->ExecuteDelete(del).first;
}

// Shorthands.
SqlResultSet SelectAll() { return SelectBy({}); }
SqlResultSet SelectBy(const std::string &col, const std::string &value) {
  return SelectBy({{col, {value}}});
}
SqlResultSet DeleteBy(const std::string &col, const std::string &value) {
  return DeleteBy({{col, {value}}});
}

}  // namespace

/*
 * Helpers outside of namespace.
 */
// Equality modulo order.
bool operator==(const SqlResultSet &set,
                const std::vector<dataflow::Record> &v2) {
  const std::vector<dataflow::Record> &v1 = set.rows();
  if (v1.size() != v2.size()) {
    return false;
  }

  for (size_t i = 0; i < v1.size(); i++) {
    if (std::count(v1.begin(), v1.end(), v1.at(i)) !=
        std::count(v2.begin(), v2.end(), v1.at(i))) {
      return false;
    }
    if (std::count(v1.begin(), v1.end(), v2.at(i)) !=
        std::count(v2.begin(), v2.end(), v2.at(i))) {
      return false;
    }
  }
  return true;
}
// Debugging.
std::ostream &operator<<(std::ostream &o,
                         const std::vector<dataflow::Record> &v) {
  o << "{";
  size_t i = 0;
  for (const dataflow::Record &r : v) {
    o << " " << r;
    if (++i < v.size()) {
      o << ",";
    }
  }
  o << " }";
  return o;
}
std::ostream &operator<<(std::ostream &o, const SqlResultSet &v) {
  o << v.rows();
  return o;
}

/*
 * End of all helpers.
 */

/*
 * google test fixture class, allows us to manage the connection and
 * initialize / destruct it for every test.
 */
class RocksdbConnectionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitializeDatabase();
    CreateTable();
    CreateNameIndex();
    InsertData();
  }

  void TearDown() override { CleanDatabase(); }
};

/*
 * Simple test.
 */
TEST_F(RocksdbConnectionTest, CreateInsertSelect) {
  // Select all records.
  EXPECT_EQ(SelectAll(), GetRecords({0, 1, 2, 3}));
}

/*
 * Simple select with project.
 */
TEST_F(RocksdbConnectionTest, SelectAllProject) {
  // Select all records with projection.
  sqlast::Select select("test_table");
  select.AddColumn("1");
  select.AddColumn("'ID'");
  select.AddColumn("name");
  SqlResultSet result = CONN->ExecuteSelect(select);

  // Expected output.
  std::vector<dataflow::Record> expected;
  dataflow::SchemaRef projected = dataflow::SchemaFactory::Create(
      {"1", "'ID'", "name"}, {CType::INT, CType::TEXT, CType::TEXT}, {});
  expected.emplace_back(projected, true, 1_s, STR("ID"), STR("user1"));
  expected.emplace_back(projected, true, 1_s, STR("ID"), STR("user2"));
  expected.emplace_back(projected, true, 1_s, STR("ID"), STR("user3"));
  expected.emplace_back(projected, true, 1_s, STR("ID"), STR("user3"));

  EXPECT_EQ(result, expected);
}

/*
 * Select / Delete by primary key.
 */
TEST_F(RocksdbConnectionTest, SelectByPK) {
  EXPECT_EQ(SelectBy("ID", "0"), GetRecords({0}));
  EXPECT_EQ(SelectBy("ID", "1"), GetRecords({1}));
  EXPECT_EQ(SelectBy("ID", "2"), GetRecords({2}));
  EXPECT_EQ(SelectBy("ID", "3"), GetRecords({3}));
  EXPECT_EQ(SelectBy("ID", "4"), EMPTY);
}
TEST_F(RocksdbConnectionTest, DeleteByPK) {
  EXPECT_EQ(DeleteBy("ID", "0"), GetRecords({0}));
  EXPECT_EQ(SelectBy("ID", "0"), EMPTY);
  EXPECT_EQ(SelectAll(), GetRecords({1, 2, 3}));

  EXPECT_EQ(DeleteBy("ID", "2"), GetRecords({2}));
  EXPECT_EQ(SelectBy("ID", "2"), EMPTY);
  EXPECT_EQ(SelectAll(), GetRecords({1, 3}));

  EXPECT_EQ(DeleteBy("ID", "3"), GetRecords({3}));
  EXPECT_EQ(SelectBy("ID", "3"), EMPTY);
  EXPECT_EQ(SelectAll(), GetRecords({1}));
}

/*
 * Select / Delete by primary key and filter.
 */
TEST_F(RocksdbConnectionTest, SelectByPKAndFilter) {
  EXPECT_EQ(SelectBy({{"ID", {"0"}}, {"name", {"'user1'"}}}), GetRecords({0}));
  EXPECT_EQ(SelectBy({{"ID", {"0"}}, {"name", {"'user2'"}}}), EMPTY);
  EXPECT_EQ(SelectBy({{"ID", {"1"}}, {"name", {"'user2'"}}}), GetRecords({1}));
  EXPECT_EQ(SelectBy({{"ID", {"10"}}, {"name", {"'user2'"}}}), EMPTY);
}

TEST_F(RocksdbConnectionTest, DeleteByPKAndFilter) {
  EXPECT_EQ(DeleteBy({{"ID", {"0"}}, {"name", {"'user1'"}}}), GetRecords({0}));
  EXPECT_EQ(SelectBy("ID", "0"), EMPTY);
  EXPECT_EQ(SelectAll(), GetRecords({1, 2, 3}));

  EXPECT_EQ(DeleteBy({{"ID", {"0"}}, {"name", {"'user2'"}}}), EMPTY);
  EXPECT_EQ(SelectBy("ID", "1"), GetRecords({1}));
  EXPECT_EQ(SelectAll(), GetRecords({1, 2, 3}));

  EXPECT_EQ(DeleteBy({{"ID", {"1"}}, {"name", {"'user2'"}}}), GetRecords({1}));
  EXPECT_EQ(SelectBy("ID", "1"), EMPTY);
  EXPECT_EQ(SelectAll(), GetRecords({2, 3}));

  EXPECT_EQ(DeleteBy({{"ID", {"10"}}, {"name", {"'user2'"}}}), EMPTY);
  EXPECT_EQ(SelectAll(), GetRecords({2, 3}));
}

/*
 * Select / Delete by non-indexed column
 */
TEST_F(RocksdbConnectionTest, SelectByNonIndex) {
  EXPECT_EQ(SelectBy("name", "'user0'"), EMPTY);
  EXPECT_EQ(SelectBy("name", "'user1'"), GetRecords({0}));
  EXPECT_EQ(SelectBy("name", "'user2'"), GetRecords({1}));
  EXPECT_EQ(SelectBy("name", "'user3'"), GetRecords({2, 3}));
}

TEST_F(RocksdbConnectionTest, DeleteByNonIndex) {
  EXPECT_EQ(DeleteBy("name", "'user0'"), EMPTY);
  EXPECT_EQ(SelectBy("name", "'user0'"), EMPTY);
  EXPECT_EQ(SelectAll(), GetRecords({0, 1, 2, 3}));

  EXPECT_EQ(DeleteBy("name", "'user1'"), GetRecords({0}));
  EXPECT_EQ(SelectBy("name", "'user1'"), EMPTY);
  EXPECT_EQ(SelectAll(), GetRecords({1, 2, 3}));

  EXPECT_EQ(DeleteBy("name", "'user3'"), GetRecords({2, 3}));
  EXPECT_EQ(SelectBy("name", "'user3'"), EMPTY);
  EXPECT_EQ(SelectAll(), GetRecords({1}));
}

/*
 * Select / Delete by indexed column
 */
TEST_F(RocksdbConnectionTest, SelectByIndex) {
  EXPECT_EQ(SelectBy("name", "'user0'"), EMPTY);
  EXPECT_EQ(SelectBy("name", "'user1'"), GetRecords({0}));
  EXPECT_EQ(SelectBy("name", "'user2'"), GetRecords({1}));
  EXPECT_EQ(SelectBy("name", "'user3'"), GetRecords({2, 3}));
}

TEST_F(RocksdbConnectionTest, DeleteByIndex) {
  EXPECT_EQ(DeleteBy("name", "'user0'"), EMPTY);
  EXPECT_EQ(SelectBy("name", "'user0'"), EMPTY);
  EXPECT_EQ(SelectAll(), GetRecords({0, 1, 2, 3}));

  EXPECT_EQ(DeleteBy("name", "'user1'"), GetRecords({0}));
  EXPECT_EQ(SelectBy("name", "'user1'"), EMPTY);
  EXPECT_EQ(SelectAll(), GetRecords({1, 2, 3}));

  EXPECT_EQ(DeleteBy("name", "'user3'"), GetRecords({2, 3}));
  EXPECT_EQ(SelectBy("name", "'user3'"), EMPTY);
  EXPECT_EQ(SelectAll(), GetRecords({1}));
}

/*
 * Select / Delete by in
 */
TEST_F(RocksdbConnectionTest, SelectByIn) {
  EXPECT_EQ(SelectBy({{"ID", {"0", "1", "3"}}}), GetRecords({0, 1, 3}));
  EXPECT_EQ(SelectBy({{"name", {"'user0'", "'user1'", "'user3'"}}}),
            GetRecords({0, 2, 3}));
  EXPECT_EQ(SelectBy({{"age", {"25", "30"}}}), GetRecords({1, 2}));
}

TEST_F(RocksdbConnectionTest, DeleteByIn) {
  InitializeDatabase();
  CreateTable();
  CreateNameIndex();
  InsertData();
  EXPECT_EQ(DeleteBy({{"ID", {"0", "1", "3"}}}), GetRecords({0, 1, 3}));
  EXPECT_EQ(SelectBy({{"name", {"'user0'", "'user3'"}}}), GetRecords({2}));

  InitializeDatabase();
  CreateTable();
  CreateNameIndex();
  InsertData();
  EXPECT_EQ(DeleteBy({{"name", {"'user0'", "'user1'", "'user3'"}}}),
            GetRecords({0, 2, 3}));
  EXPECT_EQ(SelectBy({{"name", {"'user2'", "'user1'", "'user3'"}}}),
            GetRecords({1}));

  InitializeDatabase();
  CreateTable();
  CreateNameIndex();
  InsertData();
  EXPECT_EQ(DeleteBy({{"age", {"25", "30"}}}), GetRecords({1, 2}));
  EXPECT_EQ(SelectBy({{"name", {"'user0'", "'user1'", "'user3'"}}}),
            GetRecords({0, 3}));
}

TEST_F(RocksdbConnectionTest, GetShard) {
  EXPECT_EQ(CONN->GetShard("test_table", util::ShardName("user", "1")),
            GetRecords({0}));
  EXPECT_EQ(CONN->GetShard("test_table", util::ShardName("user", "2")),
            GetRecords({1, 3}));
  EXPECT_EQ(CONN->GetShard("test_table", util::ShardName("user", "3")),
            GetRecords({2, 3}));
  EXPECT_EQ(CONN->GetShard("test_table", util::ShardName("user", "4")), EMPTY);
}

TEST_F(RocksdbConnectionTest, DeleteShard) {
  // Ensure records exist.
  EXPECT_EQ(SelectBy("ID", "0"), GetRecords({0}));
  EXPECT_EQ(SelectBy("ID", "1"), GetRecords({1}));
  EXPECT_EQ(SelectBy("ID", "2"), GetRecords({2}));
  EXPECT_EQ(SelectBy("ID", "3"), GetRecords({3}));

  // Delete user2, shared row stays with user3.
  EXPECT_EQ(CONN->DeleteShard("test_table", util::ShardName("user", "2")),
            GetRecords({1, 3}));
  EXPECT_EQ(CONN->GetShard("test_table", util::ShardName("user", "2")), EMPTY);
  EXPECT_EQ(SelectBy("ID", "0"), GetRecords({0}));
  EXPECT_EQ(SelectBy("ID", "1"), EMPTY);
  EXPECT_EQ(SelectBy("ID", "2"), GetRecords({2}));
  EXPECT_EQ(SelectBy("ID", "3"), GetRecords({3}));

  EXPECT_EQ(CONN->DeleteShard("test_table", util::ShardName("user", "3")),
            GetRecords({2, 3}));
  EXPECT_EQ(CONN->GetShard("test_table", util::ShardName("user", "3")), EMPTY);
  EXPECT_EQ(SelectBy("ID", "0"), GetRecords({0}));
  EXPECT_EQ(SelectBy("ID", "1"), EMPTY);
  EXPECT_EQ(SelectBy("ID", "2"), EMPTY);
  EXPECT_EQ(SelectBy("ID", "3"), EMPTY);

  EXPECT_EQ(CONN->DeleteShard("test_table", util::ShardName("user", "1")),
            GetRecords({0}));
  EXPECT_EQ(CONN->GetShard("test_table", util::ShardName("user", "1")), EMPTY);

  // Table is now empty!
  sqlast::Select select("test_table");
  select.AddColumn("*");
  EXPECT_EQ(CONN->ExecuteSelect(select), EMPTY);
  EXPECT_EQ(CONN->GetAll("test_table"), EMPTY);
}

TEST_F(RocksdbConnectionTest, GetAllDeleteAll) {
  // Get all records in table: shared record appears only once!
  sqlast::Select select("test_table");
  select.AddColumn("*");
  EXPECT_EQ(CONN->ExecuteSelect(select), GetRecords({0, 1, 2, 3}));
  EXPECT_EQ(CONN->GetAll("test_table"), GetRecords({0, 1, 2, 3}));

  // Delete all records in table!
  sqlast::Delete delete_("test_table");
  EXPECT_EQ(CONN->ExecuteDelete(delete_).first, GetRecords({0, 1, 2, 3}));

  // Table is now empty!
  EXPECT_EQ(CONN->ExecuteSelect(select), EMPTY);
  EXPECT_EQ(CONN->GetAll("test_table"), EMPTY);
}

}  // namespace sql
}  // namespace pelton
