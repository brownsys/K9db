#include "pelton/shards/sqlengine/create.h"

#include <filesystem>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "absl/status/statusor.h"
#include "glog/logging.h"
#include "pelton/connection.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"
#include "pelton/sqlast/parser.h"
#include "pelton/util/upgradable_lock.h"

// Has to be included after parser to avoid FAIL symbol redefinition with ANTLR.
// clang-format off
// NOLINTNEXTLINE
#include "gtest/gtest.h"
// clang-format on

#define DB_PATH "/tmp/pelton/rocksdb/shards-create-test"

namespace pelton {
namespace shards {
namespace sqlengine {

/*
 * MACROS to simplify/reduce redundancy when writing CREATE TABLE statements.
 */

#define I " int "
#define STR " text "
#define PK " PRIMARY KEY "
#define FK " REFERENCES "

using CType = sqlast::ColumnDefinition::Type;
#define INT CType::INT
#define TEXT CType::TEXT

/*
 * Parsing.
 */

namespace {

sqlast::CreateTable MakeCreate(const std::string &tbl_name,
                               std::vector<std::string> cols) {
  std::string sql = "CREATE TABLE " + tbl_name + "(";
  for (size_t i = 0; i < cols.size(); i++) {
    sql += cols.at(i);
    if (i < cols.size() - 1) {
      sql += ",";
    }
  }
  sql += ");";

  sqlast::SQLParser parser;
  auto status = parser.Parse(sql);

  sqlast::CreateTable stmt =
      *reinterpret_cast<sqlast::CreateTable *>(status->get());
  return stmt;
}

}  // namespace

/*
 * Execute a parsed create statement.
 */

namespace {

void Handle(const sqlast::CreateTable &stmt, Connection *conn) {
  util::UniqueLock lock = conn->state->WriterLock();
  auto status = create::Shard(stmt, conn, &lock);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(status->Success());
}

}  // namespace

/*
 * Ensure Shard in state is correct.
 */

namespace {

void EXPECT_SHARD(Connection *conn, const std::string &shard_name,
                  const std::string &id_column, size_t id_column_index) {
  const SharderState &state = conn->state->SharderState();
  const Shard &shard = state.GetShard(shard_name);
  EXPECT_EQ(shard.shard_kind, shard_name) << "Shard: " << shard_name;
  EXPECT_EQ(shard.id_column, id_column) << "Shard: " << shard_name;
  EXPECT_EQ(shard.id_column_index, id_column_index) << "Shard: " << shard_name;
}

void EXPECT_SHARD_OWNS(Connection *conn, const std::string &shard_name,
                       const std::vector<std::string> &tables) {
  const SharderState &state = conn->state->SharderState();
  const Shard &shard = state.GetShard(shard_name);
  std::unordered_set<std::string> other(tables.begin(), tables.end());
  EXPECT_EQ(shard.owned_tables, other) << "Shard: " << shard_name;
}
void EXPECT_SHARD_ACCESSES(Connection *conn, const std::string &shard_name,
                           const std::vector<std::string> &tables) {
  const SharderState &state = conn->state->SharderState();
  const Shard &shard = state.GetShard(shard_name);
  std::unordered_set<std::string> other(tables.begin(), tables.end());
  EXPECT_EQ(shard.accessor_tables, other) << "Shard: " << shard_name;
}

}  // namespace

/*
 * Ensure Table in state is correct.
 */

namespace {

// Ensure Table in state is corret.
void EXPECT_TABLE(Connection *conn, const std::string &table_name,
                  const std::vector<std::string> &cols,
                  const std::vector<CType> &coltypes, unsigned pk,
                  size_t owner_count, size_t accessor_count) {
  const SharderState &state = conn->state->SharderState();
  const Table &tbl = state.GetTable(table_name);
  EXPECT_EQ(tbl.table_name, table_name) << "Table: " << table_name;
  EXPECT_EQ(tbl.schema.column_names(), cols) << "Table: " << table_name;
  EXPECT_EQ(tbl.schema.column_types(), coltypes) << "Table: " << table_name;
  EXPECT_EQ(tbl.schema.keys(), std::vector<unsigned>{pk})
      << "Table: " << table_name;
  EXPECT_EQ(tbl.owners.size(), owner_count) << "Table: " << table_name;
  EXPECT_EQ(tbl.accessors.size(), accessor_count) << "Table: " << table_name;
}

void EXPECT_DIRECT(Connection *conn, const std::string &table_name, bool owner,
                   const std::string &shard, const std::string &column_name,
                   size_t column_index, CType column_type) {
  const SharderState &state = conn->state->SharderState();
  bool found = false;
  const Table &tbl = state.GetTable(table_name);
  for (const auto &tmp : owner ? tbl.owners : tbl.accessors) {
    const ShardDescriptor &des = *tmp;
    if (des.shard_kind == shard && des.type == InfoType::DIRECT) {
      DirectInfo info = std::get<DirectInfo>(des.info);
      if (info.column == column_name && info.column_index == column_index &&
          info.column_type == column_type) {
        found = true;
        break;
      }
    }
  }
  EXPECT_TRUE(found) << "DIRECT Table: " << table_name;
}

void EXPECT_TRANSITIVE(Connection *conn, const std::string &table_name,
                       bool owner, const std::string &shard,
                       const std::string &column_name, size_t column_index,
                       CType column_type, const std::string &next_table,
                       const std::string &next_column,
                       size_t next_column_index) {
  const SharderState &state = conn->state->SharderState();
  bool found = false;
  const Table &tbl = state.GetTable(table_name);
  for (const auto &tmp : owner ? tbl.owners : tbl.accessors) {
    const ShardDescriptor &des = *tmp;
    if (des.shard_kind == shard && des.type == InfoType::TRANSITIVE) {
      TransitiveInfo info = std::get<TransitiveInfo>(des.info);
      if (info.column == column_name && info.column_index == column_index &&
          info.column_type == column_type && info.next_table == next_table &&
          info.next_column == next_column &&
          info.next_column_index == next_column_index &&
          owner == info.index.has_value()) {
        found = true;
        break;
      }
    }
  }
  EXPECT_TRUE(found) << "TRANSITIVE Table: " << table_name;
}

void EXPECT_VARIABLE(Connection *conn, const std::string &table_name,
                     bool owner, const std::string &shard,
                     const std::string &column_name, size_t column_index,
                     CType column_type, const std::string &origin_relation,
                     const std::string &origin_column,
                     size_t origin_column_index) {
  const SharderState &state = conn->state->SharderState();
  bool found = false;
  const Table &tbl = state.GetTable(table_name);
  for (const auto &tmp : owner ? tbl.owners : tbl.accessors) {
    const ShardDescriptor &des = *tmp;
    if (des.shard_kind == shard && des.type == InfoType::VARIABLE) {
      VariableInfo info = std::get<VariableInfo>(des.info);
      if (info.column == column_name && info.column_index == column_index &&
          info.column_type == column_type &&
          info.origin_relation == origin_relation &&
          info.origin_column == origin_column &&
          info.origin_column_index == origin_column_index &&
          owner == info.index.has_value()) {
        found = true;
        break;
      }
    }
  }
  EXPECT_TRUE(found) << "VARIABLE Table: " << table_name;
}

}  // namespace

/*
 * google test fixture class, allows us to manage the pelton state and
 * initialize / destruct it for every test.
 */
class CreateTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::filesystem::remove_all(DB_PATH);
    this->pelton_state_ = std::make_unique<State>(3, true);
    this->pelton_state_->Initialize("shards-create-test");
  }

  void TearDown() override {
    this->pelton_state_ = nullptr;
    std::filesystem::remove_all(DB_PATH);
  }

  // Create a connection.
  Connection CreateConnection() {
    Connection connection;
    connection.state = this->pelton_state_.get();
    return connection;
  }

 private:
  std::unique_ptr<State> pelton_state_;
};

/*
 * The tests!
 */

TEST_F(CreateTest, Direct) {
  // Parse create table statements.
  sqlast::CreateTable usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  sqlast::CreateTable addr =
      MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  Handle(usr, &conn);
  Handle(addr, &conn);

  // Check shards.
  EXPECT_SHARD(&conn, "user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "user", {"user", "addr"});
  EXPECT_SHARD_ACCESSES(&conn, "user", {});

  // Check tables.
  EXPECT_TABLE(&conn, "user", {"id", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "user", true, "user", "id", 0, INT);

  EXPECT_TABLE(&conn, "addr", {"id", "uid"}, {INT, INT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "addr", true, "user", "uid", 1, INT);
}

TEST_F(CreateTest, Transitive) {
  // Parse create table statements.
  sqlast::CreateTable usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  sqlast::CreateTable addr =
      MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});
  sqlast::CreateTable nums =
      MakeCreate("phones", {"id" I PK, "aid" I FK "addr(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  Handle(usr, &conn);
  Handle(addr, &conn);
  Handle(nums, &conn);

  // Check shards.
  EXPECT_SHARD(&conn, "user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "user", {"user", "addr", "phones"});
  EXPECT_SHARD_ACCESSES(&conn, "user", {});

  // Check tables.
  EXPECT_TABLE(&conn, "user", {"id", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "user", true, "user", "id", 0, INT);

  EXPECT_TABLE(&conn, "addr", {"id", "uid"}, {INT, INT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "addr", true, "user", "uid", 1, INT);

  EXPECT_TABLE(&conn, "phones", {"id", "aid"}, {INT, INT}, 0, 1, 0);
  EXPECT_TRANSITIVE(&conn, "phones", true, "user", "aid", 1, INT, "addr", "id",
                    0);
}

TEST_F(CreateTest, DeepTransitive) {
  // Parse create table statements.
  sqlast::CreateTable usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  sqlast::CreateTable addr =
      MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});
  sqlast::CreateTable nums =
      MakeCreate("phones", {"id" I PK, "aid" I FK "addr(id)"});
  sqlast::CreateTable available =
      MakeCreate("available", {"id" I PK, "pid" I FK "phones(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  Handle(usr, &conn);
  Handle(addr, &conn);
  Handle(nums, &conn);
  Handle(available, &conn);

  // Check shards.
  EXPECT_SHARD(&conn, "user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "user", {"user", "addr", "phones", "available"});
  EXPECT_SHARD_ACCESSES(&conn, "user", {});

  // Check tables.
  EXPECT_TABLE(&conn, "user", {"id", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "user", true, "user", "id", 0, INT);

  EXPECT_TABLE(&conn, "addr", {"id", "uid"}, {INT, INT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "addr", true, "user", "uid", 1, INT);

  EXPECT_TABLE(&conn, "phones", {"id", "aid"}, {INT, INT}, 0, 1, 0);
  EXPECT_TRANSITIVE(&conn, "phones", true, "user", "aid", 1, INT, "addr", "id",
                    0);

  EXPECT_TABLE(&conn, "available", {"id", "pid"}, {INT, INT}, 0, 1, 0);
  EXPECT_TRANSITIVE(&conn, "available", true, "user", "pid", 1, INT, "phones",
                    "id", 0);
}

TEST_F(CreateTest, OwnerAndNothing) {
  // Parse create table statements.
  sqlast::CreateTable usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  sqlast::CreateTable msg = MakeCreate(
      "msg",
      {"id" I PK, "OWNER_sender" I FK "user(id)", "receiver" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  Handle(usr, &conn);
  Handle(msg, &conn);

  // Check shards.
  EXPECT_SHARD(&conn, "user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "user", {"user", "msg"});
  EXPECT_SHARD_ACCESSES(&conn, "user", {});

  // Check tables.
  EXPECT_TABLE(&conn, "user", {"id", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "user", true, "user", "id", 0, INT);

  EXPECT_TABLE(&conn, "msg", {"id", "OWNER_sender", "receiver"},
               {INT, INT, INT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "msg", true, "user", "OWNER_sender", 1, INT);
}

TEST_F(CreateTest, OwnerAccessor) {
  // Parse create table statements.
  sqlast::CreateTable usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  sqlast::CreateTable msg =
      MakeCreate("msg", {"id" I PK, "OWNER_sender" I FK "user(id)",
                         "ACCESSOR_receiver" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  Handle(usr, &conn);
  Handle(msg, &conn);

  // Check shards.
  EXPECT_SHARD(&conn, "user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "user", {"user", "msg"});
  EXPECT_SHARD_ACCESSES(&conn, "user", {"msg"});

  // Check tables.
  EXPECT_TABLE(&conn, "user", {"id", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "user", true, "user", "id", 0, INT);

  EXPECT_TABLE(&conn, "msg", {"id", "OWNER_sender", "ACCESSOR_receiver"},
               {INT, INT, INT}, 0, 1, 1);
  EXPECT_DIRECT(&conn, "msg", true, "user", "OWNER_sender", 1, INT);
  EXPECT_DIRECT(&conn, "msg", false, "user", "ACCESSOR_receiver", 2, INT);
}

TEST_F(CreateTest, OwnerLattice) {
  // Parse create table statements.
  sqlast::CreateTable usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  sqlast::CreateTable msg =
      MakeCreate("msg", {"id" I PK, "OWNER_sender" I FK "user(id)",
                         "ACCESSOR_receiver" I FK "user(id)"});
  sqlast::CreateTable meta =
      MakeCreate("metadata", {"id" I PK, "OWNER_msg" I FK "msg(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  Handle(usr, &conn);
  Handle(msg, &conn);
  Handle(meta, &conn);

  // Check shards.
  EXPECT_SHARD(&conn, "user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "user", {"user", "msg", "metadata"});
  EXPECT_SHARD_ACCESSES(&conn, "user", {"msg", "metadata"});

  // Check tables.
  EXPECT_TABLE(&conn, "user", {"id", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "user", true, "user", "id", 0, INT);

  EXPECT_TABLE(&conn, "msg", {"id", "OWNER_sender", "ACCESSOR_receiver"},
               {INT, INT, INT}, 0, 1, 1);
  EXPECT_DIRECT(&conn, "msg", true, "user", "OWNER_sender", 1, INT);
  EXPECT_DIRECT(&conn, "msg", false, "user", "ACCESSOR_receiver", 2, INT);

  EXPECT_TABLE(&conn, "metadata", {"id", "OWNER_msg"}, {INT, INT}, 0, 1, 1);
  EXPECT_TRANSITIVE(&conn, "metadata", true, "user", "OWNER_msg", 1, INT, "msg",
                    "id", 0);
  EXPECT_TRANSITIVE(&conn, "metadata", false, "user", "OWNER_msg", 1, INT,
                    "msg", "id", 0);
}

TEST_F(CreateTest, AccessorLattice) {
  // Parse create table statements.
  sqlast::CreateTable usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  sqlast::CreateTable msg =
      MakeCreate("msg", {"id" I PK, "OWNER_sender" I FK "user(id)",
                         "ACCESSOR_receiver" I FK "user(id)"});
  sqlast::CreateTable meta =
      MakeCreate("metadata", {"id" I PK, "ACCESSOR_msg" I FK "msg(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  Handle(usr, &conn);
  Handle(msg, &conn);
  Handle(meta, &conn);

  // Check shards.
  EXPECT_SHARD(&conn, "user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "user", {"user", "msg"});
  EXPECT_SHARD_ACCESSES(&conn, "user", {"msg", "metadata"});

  // Check tables.
  EXPECT_TABLE(&conn, "user", {"id", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "user", true, "user", "id", 0, INT);

  EXPECT_TABLE(&conn, "msg", {"id", "OWNER_sender", "ACCESSOR_receiver"},
               {INT, INT, INT}, 0, 1, 1);
  EXPECT_DIRECT(&conn, "msg", true, "user", "OWNER_sender", 1, INT);
  EXPECT_DIRECT(&conn, "msg", false, "user", "ACCESSOR_receiver", 2, INT);

  EXPECT_TABLE(&conn, "metadata", {"id", "ACCESSOR_msg"}, {INT, INT}, 0, 0, 2);
  EXPECT_TRANSITIVE(&conn, "metadata", false, "user", "ACCESSOR_msg", 1, INT,
                    "msg", "id", 0);
  EXPECT_TRANSITIVE(&conn, "metadata", false, "user", "ACCESSOR_msg", 1, INT,
                    "msg", "id", 0);
}

TEST_F(CreateTest, TwoOwners) {
  // Parse create table statements.
  sqlast::CreateTable usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  sqlast::CreateTable msg =
      MakeCreate("msg", {"id" I PK, "OWNER_sender" I FK "user(id)",
                         "OWNER_receiver" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  Handle(usr, &conn);
  Handle(msg, &conn);

  // Check shards.
  EXPECT_SHARD(&conn, "user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "user", {"user", "msg"});
  EXPECT_SHARD_ACCESSES(&conn, "user", {});

  // Check tables.
  EXPECT_TABLE(&conn, "user", {"id", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "user", true, "user", "id", 0, INT);

  EXPECT_TABLE(&conn, "msg", {"id", "OWNER_sender", "OWNER_receiver"},
               {INT, INT, INT}, 0, 2, 0);
  EXPECT_DIRECT(&conn, "msg", true, "user", "OWNER_sender", 1, INT);
  EXPECT_DIRECT(&conn, "msg", true, "user", "OWNER_receiver", 2, INT);
}

TEST_F(CreateTest, TwoOwnersTransitive) {
  // Parse create table statements.
  sqlast::CreateTable usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  sqlast::CreateTable msg =
      MakeCreate("msg", {"id" I PK, "OWNER_sender" I FK "user(id)",
                         "OWNER_receiver" I FK "user(id)"});
  sqlast::CreateTable delivered =
      MakeCreate("delivered", {"id" I PK, "OWNER_msg" I FK "msg(id)"});
  sqlast::CreateTable error =
      MakeCreate("error", {"id" I PK, "ACCESSOR_msg" I FK "msg(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  Handle(usr, &conn);
  Handle(msg, &conn);
  Handle(delivered, &conn);
  Handle(error, &conn);

  // Check shards.
  EXPECT_SHARD(&conn, "user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "user", {"user", "msg", "delivered"});
  EXPECT_SHARD_ACCESSES(&conn, "user", {"error"});

  // Check tables.
  EXPECT_TABLE(&conn, "user", {"id", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "user", true, "user", "id", 0, INT);

  EXPECT_TABLE(&conn, "msg", {"id", "OWNER_sender", "OWNER_receiver"},
               {INT, INT, INT}, 0, 2, 0);
  EXPECT_DIRECT(&conn, "msg", true, "user", "OWNER_sender", 1, INT);
  EXPECT_DIRECT(&conn, "msg", true, "user", "OWNER_receiver", 2, INT);

  EXPECT_TABLE(&conn, "delivered", {"id", "OWNER_msg"}, {INT, INT}, 0, 2, 0);
  EXPECT_TRANSITIVE(&conn, "delivered", true, "user", "OWNER_msg", 1, INT,
                    "msg", "id", 0);
  EXPECT_TRANSITIVE(&conn, "delivered", true, "user", "OWNER_msg", 1, INT,
                    "msg", "id", 0);

  EXPECT_TABLE(&conn, "error", {"id", "ACCESSOR_msg"}, {INT, INT}, 0, 0, 2);
  EXPECT_TRANSITIVE(&conn, "error", false, "user", "ACCESSOR_msg", 1, INT,
                    "msg", "id", 0);
  EXPECT_TRANSITIVE(&conn, "error", false, "user", "ACCESSOR_msg", 1, INT,
                    "msg", "id", 0);
}

TEST_F(CreateTest, ShardedDataSubject) {
  // Parse create table statements.
  sqlast::CreateTable usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  sqlast::CreateTable invited =
      MakeCreate("invited_user",
                 {"id" I PK, "PII_name" STR, "inviting_user" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  Handle(usr, &conn);
  Handle(invited, &conn);

  // Check shards.
  EXPECT_SHARD(&conn, "user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "user", {"user", "invited_user"});
  EXPECT_SHARD_ACCESSES(&conn, "user", {});

  EXPECT_SHARD(&conn, "invited_user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "invited_user", {"invited_user"});
  EXPECT_SHARD_ACCESSES(&conn, "invited_user", {});

  // Check tables.
  EXPECT_TABLE(&conn, "user", {"id", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "user", true, "user", "id", 0, INT);

  EXPECT_TABLE(&conn, "invited_user", {"id", "PII_name", "inviting_user"},
               {INT, TEXT, INT}, 0, 2, 0);
  EXPECT_DIRECT(&conn, "invited_user", true, "invited_user", "id", 0, INT);
  EXPECT_DIRECT(&conn, "invited_user", true, "user", "inviting_user", 2, INT);
}

TEST_F(CreateTest, VariableOwner) {
  // Parse create table statements.
  sqlast::CreateTable usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  sqlast::CreateTable grps = MakeCreate("grps", {"gid" I PK});
  sqlast::CreateTable assoc =
      MakeCreate("association", {"id" I PK, "OWNING_group" I FK "grps(gid)",
                                 "OWNER_user" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  Handle(usr, &conn);
  Handle(grps, &conn);
  Handle(assoc, &conn);

  // Check shards.
  EXPECT_SHARD(&conn, "user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "user", {"user", "grps", "association"});
  EXPECT_SHARD_ACCESSES(&conn, "user", {});

  // Check tables.
  EXPECT_TABLE(&conn, "user", {"id", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "user", true, "user", "id", 0, INT);

  EXPECT_TABLE(&conn, "grps", {"gid"}, {INT}, 0, 1, 0);
  EXPECT_VARIABLE(&conn, "grps", true, "user", "gid", 0, INT, "association",
                  "OWNING_group", 1);

  EXPECT_TABLE(&conn, "association", {"id", "OWNING_group", "OWNER_user"},
               {INT, INT, INT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "association", true, "user", "OWNER_user", 2, INT);
}

TEST_F(CreateTest, TransitiveVariableOwner) {
  // Parse create table statements.
  sqlast::CreateTable usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  sqlast::CreateTable grps = MakeCreate("grps", {"gid" I PK});
  sqlast::CreateTable assoc =
      MakeCreate("association", {"id" I PK, "OWNING_group" I FK "grps(gid)",
                                 "OWNER_user" I FK "user(id)"});
  sqlast::CreateTable files = MakeCreate("files", {"fid" I PK});
  sqlast::CreateTable fassoc =
      MakeCreate("fassociation", {"id" I PK, "OWNING_file" I FK "files(fid)",
                                  "OWNER_group" I FK "grps(gid)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  Handle(usr, &conn);
  Handle(grps, &conn);
  Handle(assoc, &conn);
  Handle(files, &conn);
  Handle(fassoc, &conn);

  // Check shards.
  EXPECT_SHARD(&conn, "user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "user",
                    {"user", "grps", "association", "files", "fassociation"});
  EXPECT_SHARD_ACCESSES(&conn, "user", {});

  // Check tables.
  EXPECT_TABLE(&conn, "user", {"id", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "user", true, "user", "id", 0, INT);

  EXPECT_TABLE(&conn, "grps", {"gid"}, {INT}, 0, 1, 0);
  EXPECT_VARIABLE(&conn, "grps", true, "user", "gid", 0, INT, "association",
                  "OWNING_group", 1);

  EXPECT_TABLE(&conn, "association", {"id", "OWNING_group", "OWNER_user"},
               {INT, INT, INT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "association", true, "user", "OWNER_user", 2, INT);

  EXPECT_TABLE(&conn, "files", {"fid"}, {INT}, 0, 1, 0);
  EXPECT_VARIABLE(&conn, "files", true, "user", "fid", 0, INT, "fassociation",
                  "OWNING_file", 1);

  EXPECT_TABLE(&conn, "fassociation", {"id", "OWNING_file", "OWNER_group"},
               {INT, INT, INT}, 0, 1, 0);
  EXPECT_TRANSITIVE(&conn, "fassociation", true, "user", "OWNER_group", 2, INT,
                    "grps", "gid", 0);
}

TEST_F(CreateTest, VariableAccessor) {
  // Parse create table statements.
  sqlast::CreateTable usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  sqlast::CreateTable grps = MakeCreate("grps", {"gid" I PK});
  sqlast::CreateTable assoc =
      MakeCreate("association", {"id" I PK, "ACCESSING_group" I FK "grps(gid)",
                                 "OWNER_user" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  Handle(usr, &conn);
  Handle(grps, &conn);
  Handle(assoc, &conn);

  // Check shards.
  EXPECT_SHARD(&conn, "user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "user", {"user", "association"});
  EXPECT_SHARD_ACCESSES(&conn, "user", {"grps"});

  // Check tables.
  EXPECT_TABLE(&conn, "user", {"id", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "user", true, "user", "id", 0, INT);

  EXPECT_TABLE(&conn, "grps", {"gid"}, {INT}, 0, 0, 1);
  EXPECT_VARIABLE(&conn, "grps", false, "user", "gid", 0, INT, "association",
                  "ACCESSING_group", 1);

  EXPECT_TABLE(&conn, "association", {"id", "ACCESSING_group", "OWNER_user"},
               {INT, INT, INT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "association", true, "user", "OWNER_user", 2, INT);
}

TEST_F(CreateTest, TransitiveVariableAccessor) {
  // Parse create table statements.
  sqlast::CreateTable usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  sqlast::CreateTable grps = MakeCreate("grps", {"gid" I PK});
  sqlast::CreateTable assoc =
      MakeCreate("association", {"id" I PK, "ACCESSING_group" I FK "grps(gid)",
                                 "OWNER_user" I FK "user(id)"});
  sqlast::CreateTable files = MakeCreate("files", {"fid" I PK});
  sqlast::CreateTable fassoc =
      MakeCreate("fassociation", {"id" I PK, "ACCESSING_file" I FK "files(fid)",
                                  "ACCESSOR_group" I FK "grps(gid)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  Handle(usr, &conn);
  Handle(grps, &conn);
  Handle(assoc, &conn);
  Handle(files, &conn);
  Handle(fassoc, &conn);

  // Check shards.
  EXPECT_SHARD(&conn, "user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "user", {"user", "association"});
  EXPECT_SHARD_ACCESSES(&conn, "user", {"grps", "files", "fassociation"});

  // Check tables.
  EXPECT_TABLE(&conn, "user", {"id", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "user", true, "user", "id", 0, INT);

  EXPECT_TABLE(&conn, "grps", {"gid"}, {INT}, 0, 0, 1);
  EXPECT_VARIABLE(&conn, "grps", false, "user", "gid", 0, INT, "association",
                  "ACCESSING_group", 1);

  EXPECT_TABLE(&conn, "association", {"id", "ACCESSING_group", "OWNER_user"},
               {INT, INT, INT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "association", true, "user", "OWNER_user", 2, INT);

  EXPECT_TABLE(&conn, "files", {"fid"}, {INT}, 0, 0, 1);
  EXPECT_VARIABLE(&conn, "files", false, "user", "fid", 0, INT, "fassociation",
                  "ACCESSING_file", 1);

  EXPECT_TABLE(&conn, "fassociation",
               {"id", "ACCESSING_file", "ACCESSOR_group"}, {INT, INT, INT}, 0,
               0, 1);
  EXPECT_TRANSITIVE(&conn, "fassociation", false, "user", "ACCESSOR_group", 2,
                    INT, "grps", "gid", 0);
}

TEST_F(CreateTest, VariableAccessorLattice) {
  // Parse create table statements.
  sqlast::CreateTable usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  sqlast::CreateTable admin = MakeCreate("admin", {"aid" I PK, "PII_name" STR});
  sqlast::CreateTable grps =
      MakeCreate("grps", {"gid" I PK, "OWNER_admin" I FK "admin(aid)"});
  sqlast::CreateTable assoc =
      MakeCreate("association", {"sid" I PK, "ACCESSING_group" I FK "grps(gid)",
                                 "OWNER_user" I FK "user(id)"});
  sqlast::CreateTable files =
      MakeCreate("files", {"fid" I PK, "OWNER_creator" I FK "user(id)"});
  sqlast::CreateTable fassoc =
      MakeCreate("fassociation", {"fsid" I PK, "OWNING_file" I FK "files(fid)",
                                  "OWNER_group" I FK "grps(gid)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  Handle(usr, &conn);
  Handle(admin, &conn);
  Handle(grps, &conn);
  Handle(assoc, &conn);
  Handle(files, &conn);
  Handle(fassoc, &conn);

  // Check shards.
  EXPECT_SHARD(&conn, "user", "id", 0);
  EXPECT_SHARD_OWNS(&conn, "user", {"user", "association", "files"});
  EXPECT_SHARD_ACCESSES(&conn, "user", {"grps", "files", "fassociation"});

  EXPECT_SHARD(&conn, "admin", "aid", 0);
  EXPECT_SHARD_OWNS(&conn, "admin", {"admin", "grps", "fassociation", "files"});
  EXPECT_SHARD_ACCESSES(&conn, "admin", {});

  // Check tables.
  EXPECT_TABLE(&conn, "user", {"id", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "user", true, "user", "id", 0, INT);

  EXPECT_TABLE(&conn, "admin", {"aid", "PII_name"}, {INT, TEXT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "admin", true, "admin", "aid", 0, INT);

  EXPECT_TABLE(&conn, "grps", {"gid", "OWNER_admin"}, {INT, INT}, 0, 1, 1);
  EXPECT_DIRECT(&conn, "grps", true, "admin", "OWNER_admin", 1, INT);
  EXPECT_VARIABLE(&conn, "grps", false, "user", "gid", 0, INT, "association",
                  "ACCESSING_group", 1);

  EXPECT_TABLE(&conn, "association", {"sid", "ACCESSING_group", "OWNER_user"},
               {INT, INT, INT}, 0, 1, 0);
  EXPECT_DIRECT(&conn, "association", true, "user", "OWNER_user", 2, INT);

  EXPECT_TABLE(&conn, "files", {"fid", "OWNER_creator"}, {INT, INT}, 0, 2, 1);
  EXPECT_DIRECT(&conn, "files", true, "user", "OWNER_creator", 1, INT);
  EXPECT_VARIABLE(&conn, "files", true, "admin", "fid", 0, INT, "fassociation",
                  "OWNING_file", 1);
  EXPECT_VARIABLE(&conn, "files", false, "user", "fid", 0, INT, "fassociation",
                  "OWNING_file", 1);

  EXPECT_TABLE(&conn, "fassociation", {"fsid", "OWNING_file", "OWNER_group"},
               {INT, INT, INT}, 0, 1, 1);
  EXPECT_TRANSITIVE(&conn, "fassociation", true, "admin", "OWNER_group", 2, INT,
                    "grps", "gid", 0);
  EXPECT_TRANSITIVE(&conn, "fassociation", false, "user", "OWNER_group", 2, INT,
                    "grps", "gid", 0);
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  ::gflags::ParseCommandLineFlags(&argc, &argv, true);
  ::google::InitGoogleLogging(argv[0]);

  return RUN_ALL_TESTS();
}
