#include <string>
#include <unordered_set>
#include <vector>

#include "glog/logging.h"
#include "k9db/shards/sqlengine/tests_helpers.h"
#include "k9db/util/shard_name.h"

namespace k9db {
namespace shards {
namespace sqlengine {

using V = std::vector<std::string>;
using VV = std::vector<std::vector<std::string>>;
using SN = util::ShardName;

/*
 * The tests!
 */

// Define a fixture that manages a k9db connection.
K9DB_FIXTURE(GDPRForgetTest);

TEST_F(GDPRForgetTest, Datasubject) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);

  // Make a k9db connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));

  // Perform some inserts.
  auto &&[stmt1, row1] = MakeInsert("user", {"0", "'u1'"});
  auto &&[stmt2, row2] = MakeInsert("user", {"1", "'u2'"});
  auto &&[stmt3, row3] = MakeInsert("user", {"2", "'u3'"});
  auto &&[stmt4, row4] = MakeInsert("user", {"5", "'u10'"});

  EXPECT_UPDATE(Execute(stmt1, &conn), 1);
  EXPECT_UPDATE(Execute(stmt2, &conn), 1);
  EXPECT_UPDATE(Execute(stmt3, &conn), 1);
  EXPECT_UPDATE(Execute(stmt4, &conn), 1);

  // Validate forget.
  std::string forget = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget, &conn), 1);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("user", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("user", SN("user", "1")), (V{row2}));
  EXPECT_EQ(db->GetShard("user", SN("user", "2")), (V{row3}));
  EXPECT_EQ(db->GetShard("user", SN("user", "5")), (V{row4}));
  EXPECT_EQ(db->GetShard("user", SN("user", "10")), (V{}));
  EXPECT_EQ(db->GetShard("user", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetTest, DirectTable) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string addr = MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});

  // Make a k9db connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(addr, &conn));

  // Perform some inserts.
  auto &&[usr1, row0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, __] = MakeInsert("user", {"5", "'u10'"});
  auto &&[addr1, row1] = MakeInsert("addr", {"0", "0"});
  auto &&[addr2, row2] = MakeInsert("addr", {"1", "0"});
  auto &&[addr3, row3] = MakeInsert("addr", {"2", "5"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr1, &conn), 1);
  EXPECT_UPDATE(Execute(addr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr3, &conn), 1);

  // Validate forget.
  std::string forget = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget, &conn), 3);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("addr", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("addr", SN("user", "5")), (V{row3}));
  EXPECT_EQ(db->GetShard("addr", SN("user", "10")), (V{}));
  EXPECT_EQ(db->GetShard("addr", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetTest, UnshardedTable) {
  // Parse create table statements.
  std::string unsharded = MakeCreate("unsharded", {"id" I PK, "name" STR});
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string addr = MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});

  // Make a k9db connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(addr, &conn));
  EXPECT_SUCCESS(Execute(unsharded, &conn));

  // Perform some inserts.
  auto &&[usr1, row0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, __] = MakeInsert("user", {"5", "'u10'"});
  auto &&[addr1, row1] = MakeInsert("addr", {"0", "0"});
  auto &&[addr2, row2] = MakeInsert("addr", {"1", "0"});
  auto &&[addr3, row3] = MakeInsert("addr", {"2", "5"});
  auto &&[stmt1, row4] = MakeInsert("unsharded", {"0", "'u1'"});
  auto &&[stmt2, row5] = MakeInsert("unsharded", {"1", "'u2'"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr1, &conn), 1);
  EXPECT_UPDATE(Execute(addr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr3, &conn), 1);
  EXPECT_UPDATE(Execute(stmt1, &conn), 1);
  EXPECT_UPDATE(Execute(stmt2, &conn), 1);

  // Validate forget.
  std::string forget = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget, &conn), 3);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("addr", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("addr", SN("user", "5")), (V{row3}));
  EXPECT_EQ(db->GetShard("addr", SN("user", "10")), (V{}));
  EXPECT_EQ(db->GetShard("addr", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetShard("unsharded", SN(DEFAULT_SHARD, DEFAULT_SHARD)),
            (V{row4, row5}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetTest, TransitiveTable) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string addr = MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});
  std::string nums = MakeCreate("phones", {"id" I PK, "aid" I FK "addr(id)"});

  // Make a k9db connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(addr, &conn));
  EXPECT_SUCCESS(Execute(nums, &conn));

  // Perform some inserts.
  auto &&[usr1, row0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, __] = MakeInsert("user", {"5", "'u10'"});
  auto &&[addr1, row1] = MakeInsert("addr", {"0", "0"});
  auto &&[addr2, row2] = MakeInsert("addr", {"1", "0"});
  auto &&[addr3, row3] = MakeInsert("addr", {"2", "5"});
  auto &&[num1, row4] = MakeInsert("phones", {"0", "2"});
  auto &&[num2, row5] = MakeInsert("phones", {"1", "1"});
  auto &&[num3, row6] = MakeInsert("phones", {"2", "0"});
  auto &&[num4, row7] = MakeInsert("phones", {"3", "0"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr1, &conn), 1);
  EXPECT_UPDATE(Execute(addr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr3, &conn), 1);
  EXPECT_UPDATE(Execute(num1, &conn), 1);
  EXPECT_UPDATE(Execute(num2, &conn), 1);
  EXPECT_UPDATE(Execute(num3, &conn), 1);
  EXPECT_UPDATE(Execute(num4, &conn), 1);

  // Validate forget.
  std::string forget = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget, &conn), 6);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("phones", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("phones", SN("user", "5")), (V{row4}));
  EXPECT_EQ(db->GetShard("phones", SN("user", "10")), (V{}));
  EXPECT_EQ(db->GetShard("phones", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetTest, DeepTransitiveTable) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string addr = MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});
  std::string nums = MakeCreate("phones", {"id" I PK, "aid" I FK "addr(id)"});
  std::string deep = MakeCreate("deep", {"id" I PK, "pid" I FK "phones(id)"});

  // Make a k9db connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(addr, &conn));
  EXPECT_SUCCESS(Execute(nums, &conn));
  EXPECT_SUCCESS(Execute(deep, &conn));

  // Perform some inserts.
  auto &&[usr1, u_] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u__] = MakeInsert("user", {"5", "'u10'"});
  auto &&[addr1, a_] = MakeInsert("addr", {"0", "0"});
  auto &&[addr2, a__] = MakeInsert("addr", {"1", "0"});
  auto &&[addr3, a___] = MakeInsert("addr", {"2", "5"});
  auto &&[num1, p_] = MakeInsert("phones", {"0", "2"});
  auto &&[num2, p__] = MakeInsert("phones", {"1", "1"});
  auto &&[num3, p___] = MakeInsert("phones", {"2", "0"});
  auto &&[num4, p____] = MakeInsert("phones", {"3", "0"});
  auto &&[deep1, row1] = MakeInsert("deep", {"0", "2"});
  auto &&[deep2, row2] = MakeInsert("deep", {"1", "2"});
  auto &&[deep3, row3] = MakeInsert("deep", {"2", "0"});
  auto &&[deep4, row4] = MakeInsert("deep", {"3", "1"});
  auto &&[deep5, row5] = MakeInsert("deep", {"4", "3"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr1, &conn), 1);
  EXPECT_UPDATE(Execute(addr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr3, &conn), 1);
  EXPECT_UPDATE(Execute(num1, &conn), 1);
  EXPECT_UPDATE(Execute(num2, &conn), 1);
  EXPECT_UPDATE(Execute(num3, &conn), 1);
  EXPECT_UPDATE(Execute(num4, &conn), 1);
  EXPECT_UPDATE(Execute(deep1, &conn), 1);
  EXPECT_UPDATE(Execute(deep2, &conn), 1);
  EXPECT_UPDATE(Execute(deep3, &conn), 1);
  EXPECT_UPDATE(Execute(deep4, &conn), 1);
  EXPECT_UPDATE(Execute(deep5, &conn), 1);

  // Validate forget.
  std::string forget = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget, &conn), 10);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("deep", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("deep", SN("user", "5")), (V{row3}));
  EXPECT_EQ(db->GetShard("deep", SN("user", "10")), (V{}));
  EXPECT_EQ(db->GetShard("deep", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetTest, TwoOwners) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "user(id)", "receiver" I OB "user(id)"});

  // Make a k9db connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(msg, &conn));

  // Perform some inserts.
  auto &&[usr1, u_] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u__] = MakeInsert("user", {"5", "'u10'"});
  auto &&[usr3, u___] = MakeInsert("user", {"10", "'u100'"});
  auto &&[msg1, row1] = MakeInsert("msg", {"1", "0", "10"});
  auto &&[msg2, row2] = MakeInsert("msg", {"2", "0", "0"});
  auto &&[msg3, row3] = MakeInsert("msg", {"3", "5", "10"});
  auto &&[msg4, row4] = MakeInsert("msg", {"4", "5", "0"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 2);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);  // Inserted only once for u1.
  EXPECT_UPDATE(Execute(msg3, &conn), 2);
  EXPECT_UPDATE(Execute(msg4, &conn), 2);

  // Validate forget for user with id 0.
  std::string forget1 = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget1, &conn), 4);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("msg", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "5")), (V{row3, row4}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "10")), (V{row1, row3}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();

  // Validate forget for user with id 10.
  std::string forget2 = MakeGDPRForget("user", "10");
  EXPECT_UPDATE(Execute(forget2, &conn), 3);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("msg", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "5")), (V{row3, row4}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "10")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetTest, TwoOwnersDataflowUpdate) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "user(id)", "receiver" I OB "user(id)"});

  // Make a k9db connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(msg, &conn));

  // Perform some inserts.
  auto &&[usr1, u_] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u__] = MakeInsert("user", {"5", "'u10'"});
  auto &&[usr3, u___] = MakeInsert("user", {"10", "'u100'"});
  auto &&[msg1, row1] = MakeInsert("msg", {"1", "0", "10"});
  auto &&[msg2, row2] = MakeInsert("msg", {"2", "0", "0"});
  auto &&[msg3, row3] = MakeInsert("msg", {"3", "5", "10"});
  auto &&[msg4, row4] = MakeInsert("msg", {"4", "5", "0"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 2);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);  // Inserted only once for u1.
  EXPECT_UPDATE(Execute(msg3, &conn), 2);
  EXPECT_UPDATE(Execute(msg4, &conn), 2);

  // Create dataflow.
  EXPECT_SUCCESS(Execute("CREATE VIEW v1 AS '\"SELECT * FROM msg\"';", &conn));

  // Validate initial dataflow state.
  EXPECT_EQ(Execute("SELECT * FROM v1;", &conn).ResultSets().at(0),
            (V{row1, row2, row3, row4}));

  // Validate forget for user with id 0.
  std::string forget1 = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget1, &conn), 4);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("msg", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "5")), (V{row3, row4}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "10")), (V{row1, row3}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();

  // Validate dataflow state after forget for user with id 0.
  EXPECT_EQ(Execute("SELECT * FROM v1;", &conn).ResultSets().at(0),
            (V{row1, row3, row4}));

  // Validate forget for user with id 10.
  std::string forget2 = MakeGDPRForget("user", "10");
  EXPECT_UPDATE(Execute(forget2, &conn), 3);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("msg", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "5")), (V{row3, row4}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "10")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();

  // Validate dataflow state after forget for user with id 10.
  EXPECT_EQ(Execute("SELECT * FROM v1;", &conn).ResultSets().at(0),
            (V{row3, row4}));
}

TEST_F(GDPRForgetTest, OwnerAccessor) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "user(id)", "receiver" I AB "user(id)"});

  // Make a k9db connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(msg, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u1] = MakeInsert("user", {"5", "'u10'"});
  auto &&[usr3, u2] = MakeInsert("user", {"10", "'u100'"});
  auto &&[msg1, row1] = MakeInsert("msg", {"1", "0", "10"});
  auto &&[msg2, row2] = MakeInsert("msg", {"2", "0", "0"});
  auto &&[msg3, row3] = MakeInsert("msg", {"3", "5", "10"});
  auto &&[msg4, row4] = MakeInsert("msg", {"4", "5", "0"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 1);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);
  EXPECT_UPDATE(Execute(msg3, &conn), 1);
  EXPECT_UPDATE(Execute(msg4, &conn), 1);

  // Validate forgets.
  std::string forget1 = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget1, &conn), 3);

  std::string forget2 = MakeGDPRForget("user", "10");
  EXPECT_UPDATE(Execute(forget2, &conn), 1);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("msg", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "5")), (V{row3, row4}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "10")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetTest, ShardedDataSubject) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string invited = MakeCreate(
      "invited", {"id" I PK, "name" STR, "inviting_user" I FK "user(id)"},
      true);

  // Make a k9db connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(invited, &conn));

  // Perform some inserts.
  auto &&[usr1, u_] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u__] = MakeInsert("user", {"5", "'u10'"});
  auto &&[inv1, row1] = MakeInsert("invited", {"0", "'i1'", "5"});
  auto &&[inv2, row2] = MakeInsert("invited", {"1", "'i10'", "0"});
  auto &&[inv3, row3] = MakeInsert("invited", {"5", "'i100'", "0"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(inv1, &conn), 2);
  EXPECT_UPDATE(Execute(inv2, &conn), 2);
  EXPECT_UPDATE(Execute(inv3, &conn), 2);

  // Validate forget of user with id 0.
  std::string forget1 = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget1, &conn), 3);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("invited", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("invited", SN("user", "5")), (V{row1}));
  EXPECT_EQ(db->GetShard("invited", SN("invited", "0")), (V{row1}));
  EXPECT_EQ(db->GetShard("invited", SN("invited", "1")), (V{row2}));
  EXPECT_EQ(db->GetShard("invited", SN("invited", "5")), (V{row3}));
  db->RollbackTransaction();

  // Validate forget of invited with id 1.
  std::string forget2 = MakeGDPRForget("invited", "1");
  EXPECT_UPDATE(Execute(forget2, &conn), 1);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("invited", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("invited", SN("user", "5")), (V{row1}));
  EXPECT_EQ(db->GetShard("invited", SN("invited", "0")), (V{row1}));
  EXPECT_EQ(db->GetShard("invited", SN("invited", "1")), (V{}));
  EXPECT_EQ(db->GetShard("invited", SN("invited", "5")), (V{row3}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetTest, VariableOwnership) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string grps = MakeCreate("grps", {"gid" I PK});
  std::string assoc = MakeCreate(
      "association",
      {"id" I PK, "group_id" I OW "grps(gid)", "user_id" I OB "user(id)"});

  // Make a k9db connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(grps, &conn));
  EXPECT_SUCCESS(Execute(assoc, &conn));

  // Perform some inserts.
  auto &&[usr1, u_] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u__] = MakeInsert("user", {"5", "'u10'"});
  auto &&[grps1, row1] = MakeInsert("grps", {"0"});
  auto &&[grps2, row2] = MakeInsert("grps", {"1"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(grps1, &conn), 1);
  EXPECT_UPDATE(Execute(grps2, &conn), 1);

  // Associate groups with some users.
  auto &&[assoc1, a_] = MakeInsert("association", {"0", "0", "0"});
  auto &&[assoc2, a__] = MakeInsert("association", {"1", "1", "0"});
  auto &&[assoc3, a___] = MakeInsert("association", {"2", "1", "5"});
  auto &&[assoc4, a____] = MakeInsert("association", {"3", "1", "5"});

  EXPECT_UPDATE(Execute(assoc1, &conn), 3);
  EXPECT_UPDATE(Execute(assoc2, &conn), 3);
  EXPECT_UPDATE(Execute(assoc3, &conn), 2);
  EXPECT_UPDATE(Execute(assoc4, &conn), 1);

  // Validate forget after the first forget.
  std::string forget1 = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget1, &conn), 5);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{row2}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();

  // Validate forget after the second forget.
  std::string forget2 = MakeGDPRForget("user", "5");
  EXPECT_UPDATE(Execute(forget2, &conn), 4);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetTest, ComplexVariableOwnership) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string admin = MakeCreate("admin", {"aid" I PK, "name" STR}, true);
  std::string grps =
      MakeCreate("grps", {"gid" I PK, "admin" I OB "admin(aid)"});
  std::string assoc = MakeCreate(
      "association",
      {"sid" I PK, "group_id" I OW "grps(gid)", "user_id" I OB "user(id)"});
  std::string files =
      MakeCreate("files", {"fid" I PK, "creator" I OB "user(id)"});
  std::string fassoc = MakeCreate(
      "fassoc",
      {"fsid" I PK, "file" I OW "files(fid)", "group_id" I OB "grps(gid)"});

  // Make a k9db connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(admin, &conn));
  EXPECT_SUCCESS(Execute(grps, &conn));
  EXPECT_SUCCESS(Execute(assoc, &conn));
  EXPECT_SUCCESS(Execute(files, &conn));
  EXPECT_SUCCESS(Execute(fassoc, &conn));

  // Perform some inserts.
  auto &&[usr1, u_] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u__] = MakeInsert("user", {"5", "'u10'"});
  auto &&[adm1, d_] = MakeInsert("admin", {"0", "'a1'"});
  auto &&[grps1, grow1] = MakeInsert("grps", {"0", "0"});
  auto &&[grps2, grow2] = MakeInsert("grps", {"1", "0"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(adm1, &conn), 1);
  EXPECT_UPDATE(Execute(grps1, &conn), 1);
  EXPECT_UPDATE(Execute(grps2, &conn), 1);

  // Insert files and fassocs but not associated to any users.
  auto &&[f1, frow1] = MakeInsert("files", {"0", "0"});
  auto &&[f2, frow2] = MakeInsert("files", {"1", "0"});
  auto &&[fa1, farow1] = MakeInsert("fassoc", {"0", "0", "1"});
  auto &&[fa2, farow2] = MakeInsert("fassoc", {"1", "1", "1"});

  EXPECT_UPDATE(Execute(f1, &conn), 1);
  EXPECT_UPDATE(Execute(f2, &conn), 1);
  EXPECT_UPDATE(Execute(fa1, &conn), 2);
  EXPECT_UPDATE(Execute(fa2, &conn), 2);

  // Associate everything with a user.
  auto &&[assoc1, a_] = MakeInsert("association", {"0", "1", "5"});

  EXPECT_UPDATE(Execute(assoc1, &conn), 6);

  // Validate first forget.
  std::string forget1 = MakeGDPRForget("user", "5");
  EXPECT_UPDATE(Execute(forget1, &conn), 7);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("grps", SN("admin", "0")), (V{grow1, grow2}));
  EXPECT_EQ(db->GetShard("files", SN("admin", "0")), (V{frow1, frow2}));
  EXPECT_EQ(db->GetShard("fassoc", SN("admin", "0")), (V{farow1, farow2}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetShard("files", SN("user", "0")), (V{frow1, frow2}));
  EXPECT_EQ(db->GetShard("files", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("files", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN("user", "5")), (V{}));
  db->RollbackTransaction();

  // Validate second forget.
  std::string forget2 = MakeGDPRForget("admin", "0");
  EXPECT_UPDATE(Execute(forget2, &conn), 7);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("grps", SN("admin", "0")), (V{}));
  EXPECT_EQ(db->GetShard("files", SN("admin", "0")), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN("admin", "0")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetShard("files", SN("user", "0")), (V{frow1, frow2}));
  EXPECT_EQ(db->GetShard("files", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("files", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN("user", "5")), (V{}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetTest, ComplexVariableOwnershipAdminDelete) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string admin = MakeCreate("admin", {"aid" I PK, "name" STR}, true);
  std::string grps =
      MakeCreate("grps", {"gid" I PK, "admin" I OB "admin(aid)"});
  std::string assoc = MakeCreate(
      "association",
      {"sid" I PK, "group_id" I OW "grps(gid)", "user_id" I OB "user(id)"});
  std::string files =
      MakeCreate("files", {"fid" I PK, "creator" I OB "user(id)"});
  std::string fassoc = MakeCreate(
      "fassoc",
      {"fsid" I PK, "file" I OW "files(fid)", "group_id" I OB "grps(gid)"});

  // Make a k9db connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(admin, &conn));
  EXPECT_SUCCESS(Execute(grps, &conn));
  EXPECT_SUCCESS(Execute(assoc, &conn));
  EXPECT_SUCCESS(Execute(files, &conn));
  EXPECT_SUCCESS(Execute(fassoc, &conn));

  // Perform some inserts.
  auto &&[usr1, u_] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u__] = MakeInsert("user", {"5", "'u10'"});
  auto &&[adm1, d_] = MakeInsert("admin", {"0", "'a1'"});
  auto &&[grps1, grow1] = MakeInsert("grps", {"0", "0"});
  auto &&[grps2, grow2] = MakeInsert("grps", {"1", "0"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(adm1, &conn), 1);
  EXPECT_UPDATE(Execute(grps1, &conn), 1);
  EXPECT_UPDATE(Execute(grps2, &conn), 1);

  // Insert files and fassocs but not associated to any users.
  auto &&[f1, frow1] = MakeInsert("files", {"0", "0"});
  auto &&[f2, frow2] = MakeInsert("files", {"1", "0"});
  auto &&[fa1, farow1] = MakeInsert("fassoc", {"0", "0", "1"});
  auto &&[fa2, farow2] = MakeInsert("fassoc", {"1", "1", "1"});

  EXPECT_UPDATE(Execute(f1, &conn), 1);
  EXPECT_UPDATE(Execute(f2, &conn), 1);
  EXPECT_UPDATE(Execute(fa1, &conn), 2);
  EXPECT_UPDATE(Execute(fa2, &conn), 2);

  // Associate everything with a user.
  auto &&[assoc1, a_] = MakeInsert("association", {"0", "1", "5"});

  EXPECT_UPDATE(Execute(assoc1, &conn), 6);

  // Validate forget.
  std::string forget = MakeGDPRForget("admin", "0");
  EXPECT_UPDATE(Execute(forget, &conn), 7);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("grps", SN("admin", "0")), (V{}));
  EXPECT_EQ(db->GetShard("files", SN("admin", "0")), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN("admin", "0")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{grow2}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetShard("files", SN("user", "0")), (V{frow1, frow2}));
  EXPECT_EQ(db->GetShard("files", SN("user", "5")), (V{frow1, frow2}));
  EXPECT_EQ(db->GetShard("files", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN("user", "5")), (V{farow1, farow2}));
  db->RollbackTransaction();
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
