// clang-format off
// NOLINTNEXTLINE
#include "k9db/sql/rocksdb/rocksdb_connection.h"
// clang-format on

#include <algorithm>
// NOLINTNEXTLINE
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
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/sql/result.h"
#include "k9db/sqlast/ast.h"
#include "k9db/util/ints.h"
#include "k9db/util/iterator.h"
#include "k9db/util/shard_name.h"

/*
 * Helpers for logging and comparing resultsets and records.
 */
namespace k9db {
namespace sql {

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
bool operator==(const SqlDeleteSet &set,
                const std::vector<dataflow::Record> &v2) {
  auto iterable = set.IterateRecords();
  auto [b, e] =
      util::Map(&iterable, [](const dataflow::Record &r) { return r.Copy(); });

  std::vector<dataflow::Record> v1;
  v1.insert(v1.end(), b, e);
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

}  // namespace sql
}  // namespace k9db

/*
 * The tests.
 */
namespace k9db {
namespace sql {
namespace rocks {

/*
 * Global configuration.
 */
using CType = sqlast::ColumnDefinition::Type;

#define DB_NAME "rocksdbconnection-test"
#define DB_PATH "/tmp/k9db_rocksdbconnection-test"
#define STR(s) std::make_unique<std::string>(s)

#define SQL(s) sqlast::Value::FromSQLString(s)
#define COL(s) sqlast::Select::ResultColumn(s)
#define LIT(s) sqlast::Select::ResultColumn(SQL(s))

/*
 * google test fixture class, allows us to manage the connection and
 * initialize / destruct it for every test. An instance of the fixture class
 * is created then destroyed for each test. Global state must be stored
 * elsewhere.
 */
namespace {

class GlobalTestState {
 public:
  dataflow::SchemaRef schema_;
  std::vector<dataflow::Record> records_;
  std::unique_ptr<RocksdbConnection> conn_;

  GlobalTestState() {
    this->InitializeSchema();
    this->InitializeRecords();
    this->InitializeDatabase();
    this->CreateTable();
    this->CreateNameIndex();
    this->InsertData();
  }

  ~GlobalTestState() { this->DestroyDatabase(); }

  void InitializeSchema() {
    std::vector<std::string> columns = {"ID", "name", "age"};
    std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
    std::vector<size_t> keys = {0};
    this->schema_ = dataflow::SchemaFactory::Create(columns, types, {0});
  }

  void InitializeRecords() {
    this->records_.emplace_back(this->schema_, false, 0_s, STR("user1"), 20_s);
    this->records_.emplace_back(this->schema_, false, 1_s, STR("user2"), 25_s);
    this->records_.emplace_back(this->schema_, false, 2_s, STR("user3"), 30_s);
    this->records_.emplace_back(this->schema_, false, 3_s, STR("user3"), 35_s);
  }

  void InitializeDatabase() {
    // Drop the database (if it exists).
    std::filesystem::remove_all(DB_PATH);
    // Initialize a new connection.
    this->conn_ = std::make_unique<RocksdbConnection>();
    this->conn_->Open(DB_NAME, "");
  }

  void CreateTable() {
    auto pk = sqlast::ColumnConstraint::Type::PRIMARY_KEY;
    sqlast::CreateTable tbl("test_table");
    tbl.AddColumn("ID", sqlast::ColumnDefinition("ID", CType::INT));
    tbl.AddColumn("name", sqlast::ColumnDefinition("name", CType::TEXT));
    tbl.AddColumn("age", sqlast::ColumnDefinition("age", CType::INT));
    tbl.MutableColumn("ID").AddConstraint(sqlast::ColumnConstraint::Make(pk));
    EXPECT_TRUE(this->conn_->ExecuteCreateTable(tbl));
  }

  void CreateNameIndex() {
    // Create a rocksdb index over the name column.
    sqlast::CreateIndex idx("name_index", "test_table");
    idx.AddColumn("name");
    EXPECT_TRUE(this->conn_->ExecuteCreateIndex(idx));
  }

  void InsertData() {
    auto session = this->conn_->OpenSession();
    session->BeginTransaction(true);
    // Insert into table.
    sqlast::Insert insert("test_table");
    insert.SetValues({SQL("0"), SQL("'user1'"), SQL("20")});
    EXPECT_EQ(session->ExecuteInsert(insert, util::ShardName("user", "1")), 1);
    insert.SetValues({SQL("1"), SQL("'user2'"), SQL("25")});
    EXPECT_EQ(session->ExecuteInsert(insert, util::ShardName("user", "2")), 1);
    insert.SetValues({SQL("2"), SQL("'user3'"), SQL("30")});
    EXPECT_EQ(session->ExecuteInsert(insert, util::ShardName("user", "3")), 1);
    insert.SetValues({SQL("3"), SQL("'user3'"), SQL("35")});
    EXPECT_EQ(session->ExecuteInsert(insert, util::ShardName("user", "3")), 1);
    insert.SetValues({SQL("3"), SQL("'user3'"), SQL("35")});
    EXPECT_EQ(session->ExecuteInsert(insert, util::ShardName("user", "2")), 1);
    // Commit here to test interaction of batch with previous committed changes.
    session->CommitTransaction();
  }

  void DestroyDatabase() {
    if (this->conn_ != nullptr) {
      this->conn_->Close();
      this->conn_ = nullptr;
    }
    // Drop the database (if it exists).
    std::filesystem::remove_all(DB_PATH);
  }
};

class RocksdbConnectionTest : public ::testing::Test {
 protected:
  static std::unique_ptr<GlobalTestState> STATE;
  static size_t COUNT;
  std::unique_ptr<Session> session;

  // Setup the database before every test.
  void SetUp() override {
    if (COUNT++ == 0) {
      // Initialize the database before the first test only.
      STATE = std::make_unique<GlobalTestState>();
    }
    this->session = STATE->conn_->OpenSession();
    this->session->BeginTransaction(true);
    CHECK_LE(COUNT, 20u);
  }

  // Clean up the database after every test.
  void TearDown() override {
    // Rollback session after test (so that the next test sees the original
    // unmodified DB).
    this->session->RollbackTransaction();
    this->session = nullptr;
    // Only destroy the database when ALL tests are done.
    if (COUNT == 20) {
      STATE->DestroyDatabase();
      STATE = nullptr;
    }
  }

  // Get any subset of records.
  std::vector<dataflow::Record> GetRecords(const std::vector<int> &ids) {
    std::vector<dataflow::Record> result;
    for (int i : ids) {
      result.push_back(STATE->records_.at(i).Copy());
    }
    return result;
  }

  // Helpers for select/delete with where clauses.
  using Constraint = sqlast::ColumnConstraint::Type;
  using EType = sqlast::Expression::Type;
  using BinExpr = sqlast::BinaryExpression;
  using ColExpr = sqlast::ColumnExpression;
  using LitExpr = sqlast::LiteralExpression;
  using ListExpr = sqlast::LiteralListExpression;

  // Transform CondMao to a where condition.
  using CondMap = std::unordered_map<std::string, std::vector<std::string>>;
  std::unique_ptr<BinExpr> MakeWhere(const CondMap &map) {
    // Transform map to condition.
    std::unique_ptr<BinExpr> condition(nullptr);
    for (const auto &[col, unparsed] : map) {
      // Transform strings to typed AST values.
      std::vector<sqlast::Value> vals;
      for (const std::string &str : unparsed) {
        vals.push_back(SQL(str));
      }
      // Transform current clause.
      std::unique_ptr<BinExpr> clause(nullptr);
      if (vals.size() > 1) {
        clause = std::make_unique<BinExpr>(EType::IN);
        clause->SetLeft(std::make_unique<ColExpr>(col));
        clause->SetRight(std::make_unique<ListExpr>(vals));
      } else if (vals.front().IsNull()) {
        clause = std::make_unique<BinExpr>(EType::IS);
        clause->SetLeft(std::make_unique<ColExpr>(col));
        clause->SetRight(std::make_unique<LitExpr>(vals.front()));
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

  // Shorthands.
  SqlResultSet SelectBy(const CondMap &map) {
    sqlast::Select select("test_table");
    select.AddColumn(COL("*"));
    select.SetWhereClause(MakeWhere(map));
    return this->session->ExecuteSelect(select);
  }
  SqlDeleteSet DeleteBy(const CondMap &map) {
    sqlast::Delete del("test_table");
    del.SetWhereClause(MakeWhere(map));
    return this->session->ExecuteDelete(del);
  }
  SqlResultSet SelectBy(const std::string &col, const std::string &value) {
    return SelectBy({{col, {value}}});
  }
  SqlDeleteSet DeleteBy(const std::string &col, const std::string &value) {
    return DeleteBy({{col, {value}}});
  }
  SqlResultSet SelectAll() { return SelectBy({}); }
  SqlDeleteSet DeleteAll() { return DeleteBy({}); }
};

// Initialize static state.
std::unique_ptr<GlobalTestState> RocksdbConnectionTest::STATE = nullptr;
size_t RocksdbConnectionTest::COUNT = 0;

}  // namespace

/*
 * End of google test fixture.
 */

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
  select.AddColumn(LIT("1"));
  select.AddColumn(LIT("'ID'"));
  select.AddColumn(COL("name"));
  SqlResultSet result = session->ExecuteSelect(select);

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
  EXPECT_EQ(SelectBy("ID", "4"), GetRecords({}));
}
TEST_F(RocksdbConnectionTest, DeleteByPK) {
  EXPECT_EQ(DeleteBy("ID", "0"), GetRecords({0}));
  EXPECT_EQ(SelectBy("ID", "0"), GetRecords({}));
  EXPECT_EQ(SelectAll(), GetRecords({1, 2, 3}));

  EXPECT_EQ(DeleteBy("ID", "2"), GetRecords({2}));
  EXPECT_EQ(SelectBy("ID", "2"), GetRecords({}));
  EXPECT_EQ(SelectAll(), GetRecords({1, 3}));

  EXPECT_EQ(DeleteBy("ID", "3"), GetRecords({3}));
  EXPECT_EQ(SelectBy("ID", "3"), GetRecords({}));
  EXPECT_EQ(SelectAll(), GetRecords({1}));
}

/*
 * Select / Delete by primary key and filter.
 */
TEST_F(RocksdbConnectionTest, SelectByPKAndFilter) {
  EXPECT_EQ(SelectBy({{"ID", {"0"}}, {"name", {"'user1'"}}}), GetRecords({0}));
  EXPECT_EQ(SelectBy({{"ID", {"0"}}, {"name", {"'user2'"}}}), GetRecords({}));
  EXPECT_EQ(SelectBy({{"ID", {"1"}}, {"name", {"'user2'"}}}), GetRecords({1}));
  EXPECT_EQ(SelectBy({{"ID", {"10"}}, {"name", {"'user2'"}}}), GetRecords({}));
}

TEST_F(RocksdbConnectionTest, DeleteByPKAndFilter) {
  EXPECT_EQ(DeleteBy({{"ID", {"0"}}, {"name", {"'user1'"}}}), GetRecords({0}));
  EXPECT_EQ(SelectBy("ID", "0"), GetRecords({}));
  EXPECT_EQ(SelectAll(), GetRecords({1, 2, 3}));

  EXPECT_EQ(DeleteBy({{"ID", {"0"}}, {"name", {"'user2'"}}}), GetRecords({}));
  EXPECT_EQ(SelectBy("ID", "1"), GetRecords({1}));
  EXPECT_EQ(SelectAll(), GetRecords({1, 2, 3}));

  EXPECT_EQ(DeleteBy({{"ID", {"1"}}, {"name", {"'user2'"}}}), GetRecords({1}));
  EXPECT_EQ(SelectBy("ID", "1"), GetRecords({}));
  EXPECT_EQ(SelectAll(), GetRecords({2, 3}));

  EXPECT_EQ(DeleteBy({{"ID", {"10"}}, {"name", {"'user2'"}}}), GetRecords({}));
  EXPECT_EQ(SelectAll(), GetRecords({2, 3}));
}

/*
 * Select / Delete by non-indexed column
 */
TEST_F(RocksdbConnectionTest, SelectByNonIndex) {
  EXPECT_EQ(SelectBy("name", "'user0'"), GetRecords({}));
  EXPECT_EQ(SelectBy("name", "'user1'"), GetRecords({0}));
  EXPECT_EQ(SelectBy("name", "'user2'"), GetRecords({1}));
  EXPECT_EQ(SelectBy("name", "'user3'"), GetRecords({2, 3}));
}

TEST_F(RocksdbConnectionTest, DeleteByNonIndex) {
  EXPECT_EQ(DeleteBy("name", "'user0'"), GetRecords({}));
  EXPECT_EQ(SelectBy("name", "'user0'"), GetRecords({}));
  EXPECT_EQ(SelectAll(), GetRecords({0, 1, 2, 3}));

  EXPECT_EQ(DeleteBy("name", "'user1'"), GetRecords({0}));
  EXPECT_EQ(SelectBy("name", "'user1'"), GetRecords({}));
  EXPECT_EQ(SelectAll(), GetRecords({1, 2, 3}));

  EXPECT_EQ(DeleteBy("name", "'user3'"), GetRecords({2, 3}));
  EXPECT_EQ(SelectBy("name", "'user3'"), GetRecords({}));
  EXPECT_EQ(SelectAll(), GetRecords({1}));
}

/*
 * Select / Delete by indexed column
 */
TEST_F(RocksdbConnectionTest, SelectByIndex) {
  EXPECT_EQ(SelectBy("name", "'user0'"), GetRecords({}));
  EXPECT_EQ(SelectBy("name", "'user1'"), GetRecords({0}));
  EXPECT_EQ(SelectBy("name", "'user2'"), GetRecords({1}));
  EXPECT_EQ(SelectBy("name", "'user3'"), GetRecords({2, 3}));
}

TEST_F(RocksdbConnectionTest, DeleteByIndex) {
  EXPECT_EQ(DeleteBy("name", "'user0'"), GetRecords({}));
  EXPECT_EQ(SelectBy("name", "'user0'"), GetRecords({}));
  EXPECT_EQ(SelectAll(), GetRecords({0, 1, 2, 3}));

  EXPECT_EQ(DeleteBy("name", "'user1'"), GetRecords({0}));
  EXPECT_EQ(SelectBy("name", "'user1'"), GetRecords({}));
  EXPECT_EQ(SelectAll(), GetRecords({1, 2, 3}));

  EXPECT_EQ(DeleteBy("name", "'user3'"), GetRecords({2, 3}));
  EXPECT_EQ(SelectBy("name", "'user3'"), GetRecords({}));
  EXPECT_EQ(SelectAll(), GetRecords({1}));
}

/*
 * Select / Delete by in.
 */
TEST_F(RocksdbConnectionTest, SelectByIDIn) {
  EXPECT_EQ(SelectBy({{"ID", {"0", "1", "3"}}}), GetRecords({0, 1, 3}));
}
TEST_F(RocksdbConnectionTest, SelectByNameIn) {
  EXPECT_EQ(SelectBy({{"name", {"'user0'", "'user1'", "'user3'"}}}),
            GetRecords({0, 2, 3}));
}
TEST_F(RocksdbConnectionTest, SelectByAgeInNoIndex) {
  EXPECT_EQ(SelectBy({{"age", {"25", "30"}}}), GetRecords({1, 2}));
}

TEST_F(RocksdbConnectionTest, DeleteByIDIn) {
  EXPECT_EQ(DeleteBy({{"ID", {"0", "1", "3"}}}), GetRecords({0, 1, 3}));
  EXPECT_EQ(SelectBy({{"name", {"'user0'", "'user3'"}}}), GetRecords({2}));
}
TEST_F(RocksdbConnectionTest, DeleteByNameIn) {
  EXPECT_EQ(DeleteBy({{"name", {"'user0'", "'user1'", "'user3'"}}}),
            GetRecords({0, 2, 3}));
  EXPECT_EQ(SelectBy({{"name", {"'user2'", "'user1'", "'user3'"}}}),
            GetRecords({1}));
}
TEST_F(RocksdbConnectionTest, DeleteByAgeInNoIndex) {
  EXPECT_EQ(DeleteBy({{"age", {"25", "30"}}}), GetRecords({1, 2}));
  EXPECT_EQ(SelectBy({{"name", {"'user0'", "'user1'", "'user3'"}}}),
            GetRecords({0, 3}));
}

/*
 * GDPR operations.
 */
TEST_F(RocksdbConnectionTest, GetShard) {
  EXPECT_EQ(session->GetShard("test_table", util::ShardName("user", "1")),
            GetRecords({0}));
  EXPECT_EQ(session->GetShard("test_table", util::ShardName("user", "2")),
            GetRecords({1, 3}));
  EXPECT_EQ(session->GetShard("test_table", util::ShardName("user", "3")),
            GetRecords({2, 3}));
  EXPECT_EQ(session->GetShard("test_table", util::ShardName("user", "4")),
            GetRecords({}));
}
TEST_F(RocksdbConnectionTest, DeleteShard) {
  // Ensure records exist.
  EXPECT_EQ(SelectBy("ID", "0"), GetRecords({0}));
  EXPECT_EQ(SelectBy("ID", "1"), GetRecords({1}));
  EXPECT_EQ(SelectBy("ID", "2"), GetRecords({2}));
  EXPECT_EQ(SelectBy("ID", "3"), GetRecords({3}));

  // Delete user2, shared row stays with user3.
  EXPECT_EQ(session->DeleteShard("test_table", util::ShardName("user", "2")),
            GetRecords({1, 3}));

  EXPECT_EQ(session->GetShard("test_table", util::ShardName("user", "2")),
            GetRecords({}));
  EXPECT_EQ(SelectBy("ID", "0"), GetRecords({0}));
  EXPECT_EQ(SelectBy("ID", "1"), GetRecords({}));
  EXPECT_EQ(SelectBy("ID", "2"), GetRecords({2}));
  EXPECT_EQ(SelectBy("ID", "3"), GetRecords({3}));

  EXPECT_EQ(session->DeleteShard("test_table", util::ShardName("user", "3")),
            GetRecords({2, 3}));

  EXPECT_EQ(session->GetShard("test_table", util::ShardName("user", "3")),
            GetRecords({}));
  EXPECT_EQ(SelectBy("ID", "0"), GetRecords({0}));
  EXPECT_EQ(SelectBy("ID", "1"), GetRecords({}));
  EXPECT_EQ(SelectBy("ID", "2"), GetRecords({}));
  EXPECT_EQ(SelectBy("ID", "3"), GetRecords({}));

  EXPECT_EQ(session->DeleteShard("test_table", util::ShardName("user", "1")),
            GetRecords({0}));

  EXPECT_EQ(session->GetShard("test_table", util::ShardName("user", "1")),
            GetRecords({}));

  // Table is now empty!
  EXPECT_EQ(SelectAll(), GetRecords({}));
  EXPECT_EQ(session->GetAll("test_table"), GetRecords({}));
}

/*
 * Get all data and delete all data.
 */
TEST_F(RocksdbConnectionTest, GetAllDeleteAll) {
  // Get all records in table: shared record appears only once!
  EXPECT_EQ(SelectAll(), GetRecords({0, 1, 2, 3}));
  EXPECT_EQ(session->GetAll("test_table"), GetRecords({0, 1, 2, 3}));

  // Delete all records in table!
  EXPECT_EQ(DeleteAll(), GetRecords({0, 1, 2, 3}));

  // Table is now empty!
  EXPECT_EQ(SelectAll(), GetRecords({}));
  EXPECT_EQ(session->GetAll("test_table"), GetRecords({}));
}

/*
 * Test CountShards(...).
 */
TEST_F(RocksdbConnectionTest, CountShards) {
  // 3 -> 2, 0, 1, 2, -> 1
  std::vector<sqlast::Value> pk_values;
  pk_values.emplace_back(0_s);
  pk_values.emplace_back(1_s);
  pk_values.emplace_back(2_s);
  pk_values.emplace_back(3_s);
  pk_values.emplace_back(4_s);

  EXPECT_EQ(session->CountShards("test_table", pk_values),
            (std::vector<size_t>{1, 1, 1, 2, 0}));

  std::swap(pk_values.at(0), pk_values.at(4));
  std::swap(pk_values.at(2), pk_values.at(3));
  pk_values.pop_back();

  EXPECT_EQ(session->CountShards("test_table", pk_values),
            (std::vector<size_t>{0, 1, 2, 1}));
}

}  // namespace rocks
}  // namespace sql
}  // namespace k9db
