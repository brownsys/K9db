#include "pelton/shards/sqlengine/update.h"

#include <string>
#include <unordered_set>
#include <vector>

#include "glog/logging.h"
#include "pelton/shards/sqlengine/engine.h"
#include "pelton/shards/sqlengine/insert.h"
#include "pelton/shards/sqlengine/tests_helpers.h"
#include "pelton/util/shard_name.h"

namespace pelton {
namespace shards {
namespace sqlengine {

using V = std::vector<std::string>;
using SN = util::ShardName;

/*
 * The tests!
 */

// Define a fixture that manages a pelton connection.
PELTON_FIXTURE(UpdateTest);

TEST_F(UpdateTest, UnshardedTable) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR});

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(
      Execute("CREATE VIEW v1 AS '\"SELECT * FROM `user`\"';", &conn));

  // Perform some inserts.
  auto &&[stmt1, row1] = MakeInsert("user", {"0", "'u1'"});
  auto &&[stmt2, row2] = MakeInsert("user", {"1", "'u2'"});
  auto &&[stmt3, row3] = MakeInsert("user", {"2", "'u3'"});
  auto &&[stmt4, row4] = MakeInsert("user", {"5", "'u10'"});

  // Update one field.
  auto update1 = MakeUpdate("user", {{"name", "'u5'"}}, {"id = 0"});
  // Update two records to have the same field.
  auto update2 = MakeUpdate("user", {{"name", "'u7'"}}, {"id IN (1, 2)"});
  // Update two values in one statement.
  auto update3 = MakeUpdate("user", {{"name", "'u8'"}}, {"name = 'u7'"});

  EXPECT_UPDATE(Execute(stmt1, &conn), 1);
  EXPECT_UPDATE(Execute(stmt2, &conn), 1);
  EXPECT_UPDATE(Execute(stmt3, &conn), 1);
  EXPECT_UPDATE(Execute(stmt4, &conn), 1);
  EXPECT_QUERY(Execute("SELECT * FROM v1", &conn), (V{row1, row2, row3, row4}));

  // Perform updates.
  EXPECT_UPDATE(Execute(update1, &conn), 1);
  EXPECT_UPDATE(Execute(update2, &conn), 2);
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetAll("user"), (V{"|0|u5|", "|1|u7|", "|2|u7|", "|5|u10|"}));
  db->RollbackTransaction();
  EXPECT_QUERY(Execute("SELECT * FROM v1", &conn),
               (V{"|0|u5|", "|1|u7|", "|2|u7|", "|5|u10|"}));

  EXPECT_UPDATE(Execute(update3, &conn), 2);
  EXPECT_UPDATE(Execute(update3, &conn), 0);
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetAll("user"), (V{"|0|u5|", "|1|u8|", "|2|u8|", "|5|u10|"}));
  db->RollbackTransaction();
  EXPECT_QUERY(Execute("SELECT * FROM v1", &conn),
               (V{"|0|u5|", "|1|u8|", "|2|u8|", "|5|u10|"}));
}

TEST_F(UpdateTest, Datasubject) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(
      Execute("CREATE VIEW v1 AS '\"SELECT * FROM `user`\"';", &conn));

  // Perform some inserts.
  auto &&[stmt1, row1] = MakeInsert("user", {"0", "'u1'"});
  auto &&[stmt2, row2] = MakeInsert("user", {"1", "'u2'"});
  auto &&[stmt3, row3] = MakeInsert("user", {"2", "'u3'"});
  auto &&[stmt4, row4] = MakeInsert("user", {"5", "'u10'"});

  auto update1 = MakeUpdate("user", {{"name", "'u5'"}}, {"id = 0"});
  auto update2 = MakeUpdate("user", {{"name", "'u7'"}}, {"id IN (1, 2)"});
  auto update3 = MakeUpdate("user", {{"name", "'u8'"}}, {"name = 'u7'"});

  EXPECT_UPDATE(Execute(stmt1, &conn), 1);
  EXPECT_UPDATE(Execute(stmt2, &conn), 1);
  EXPECT_UPDATE(Execute(stmt3, &conn), 1);
  EXPECT_UPDATE(Execute(stmt4, &conn), 1);

  // Perform the updates.
  EXPECT_UPDATE(Execute(update1, &conn), 1);
  EXPECT_UPDATE(Execute(update2, &conn), 2);
  EXPECT_UPDATE(Execute(update3, &conn), 2);
  EXPECT_UPDATE(Execute(update3, &conn), 0);

  // Validate insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("user", SN("user", "0")), (V{"|0|u5|"}));
  EXPECT_EQ(db->GetShard("user", SN("user", "1")), (V{"|1|u8|"}));
  EXPECT_EQ(db->GetShard("user", SN("user", "2")), (V{"|2|u8|"}));
  EXPECT_EQ(db->GetShard("user", SN("user", "5")), (V{"|5|u10|"}));
  EXPECT_EQ(db->GetShard("user", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetAll("user"), (V{"|0|u5|", "|1|u8|", "|2|u8|", "|5|u10|"}));
  db->RollbackTransaction();
  EXPECT_QUERY(Execute("SELECT * FROM v1", &conn),
               (V{"|0|u5|", "|1|u8|", "|2|u8|", "|5|u10|"}));
}

TEST_F(UpdateTest, DirectTable) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string addr =
      MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)", "dat" I});

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(addr, &conn));
  EXPECT_SUCCESS(
      Execute("CREATE VIEW v1 AS '\"SELECT * FROM `user`\"';", &conn));
  EXPECT_SUCCESS(Execute("CREATE VIEW v2 AS '\"SELECT * FROM addr\"';", &conn));

  // Perform some inserts.
  auto &&[usr1, _] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, __] = MakeInsert("user", {"5", "'u10'"});
  auto &&[addr1, row1] = MakeInsert("addr", {"0", "5", "0"});
  auto &&[addr2, row2] = MakeInsert("addr", {"1", "0", "1"});
  auto &&[addr3, row3] = MakeInsert("addr", {"2", "5", "2"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr1, &conn), 1);
  EXPECT_UPDATE(Execute(addr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr3, &conn), 1);

  // Update a useless column, update a column that changes sharding.
  // Updates breaking FK integrity.
  auto update1 = MakeUpdate("addr", {{"dat", "dat + 10"}}, {"id = 0"});
  auto update2 = MakeUpdate("addr", {{"uid", "5"}}, {"dat = 1"});
  auto update3 = MakeUpdate("addr", {{"uid", "0"}}, {"dat IN (1, 10)"});
  auto update4 = MakeUpdate("addr", {{"uid", "100"}}, {"id = 0"});
  auto update5 = MakeUpdate("user", {{"name", "'u100'"}}, {"id = 5"});
  auto update6 = MakeUpdate("user", {{"id", "20"}}, {"id = 5"});

  // Perform the update.
  EXPECT_UPDATE(Execute(update1, &conn), 1);
  EXPECT_UPDATE(Execute(update2, &conn), 2);  // Del from old shard, add to new.
  EXPECT_UPDATE(Execute(update3, &conn), 4);  // Ditto.
  EXPECT_TRUE(ExecuteError(update4, &conn));
  EXPECT_UPDATE(Execute(update5, &conn), 1);
  EXPECT_TRUE(ExecuteError(update6, &conn));

  // Validate insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("addr", SN("user", "0")), (V{"|0|0|10|", "|1|0|1|"}));
  EXPECT_EQ(db->GetShard("addr", SN("user", "5")), (V{"|2|5|2|"}));
  EXPECT_EQ(db->GetShard("addr", SN("user", "100")), (V{}));
  EXPECT_EQ(db->GetShard("addr", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetAll("addr"), (V{"|0|0|10|", "|1|0|1|", "|2|5|2|"}));
  EXPECT_EQ(db->GetShard("user", SN("user", "0")), (V{"|0|u1|"}));
  EXPECT_EQ(db->GetShard("user", SN("user", "5")), (V{"|5|u100|"}));
  EXPECT_EQ(db->GetShard("user", SN("user", "20")), (V{}));
  EXPECT_EQ(db->GetAll("user"), (V{"|0|u1|", "|5|u100|"}));
  db->RollbackTransaction();

  // Check views.
  EXPECT_QUERY(Execute("SELECT * FROM v1", &conn), (V{"|0|u1|", "|5|u100|"}));
  EXPECT_QUERY(Execute("SELECT * FROM v2", &conn),
               (V{"|0|0|10|", "|1|0|1|", "|2|5|2|"}));
}

TEST_F(UpdateTest, TransitiveTable) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string addr =
      MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)", "dat" I});
  std::string nums = MakeCreate("phones", {"id" I PK, "aid" I FK "addr(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(addr, &conn));
  EXPECT_SUCCESS(Execute(nums, &conn));

  // Perform some inserts.
  auto &&[usr1, _] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, __] = MakeInsert("user", {"5", "'u10'"});
  auto &&[addr1, ___] = MakeInsert("addr", {"0", "0", "20"});
  auto &&[addr2, ____] = MakeInsert("addr", {"1", "0", "30"});
  auto &&[addr3, _____] = MakeInsert("addr", {"2", "5", "40"});
  auto &&[num1, row1] = MakeInsert("phones", {"0", "2"});
  auto &&[num2, row2] = MakeInsert("phones", {"1", "1"});
  auto &&[num3, row3] = MakeInsert("phones", {"2", "0"});
  auto &&[num4, row4] = MakeInsert("phones", {"3", "0"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr1, &conn), 1);
  EXPECT_UPDATE(Execute(addr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr3, &conn), 1);
  EXPECT_UPDATE(Execute(num1, &conn), 1);
  EXPECT_UPDATE(Execute(num2, &conn), 1);
  EXPECT_UPDATE(Execute(num3, &conn), 1);
  EXPECT_UPDATE(Execute(num4, &conn), 1);

  // Update FK in a transitively sharded table
  //    (should move transitively owned records to a different shard too).
  auto update1 = MakeUpdate("addr", {{"uid", "5"}}, {"id = 0"});
  auto update2 = MakeUpdate("addr", {{"dat", "400"}}, {"dat IN (40, 100)"});
  auto update3 = MakeUpdate("phones", {{"aid", "1"}}, {"aid = 2"});
  EXPECT_UPDATE(Execute(update1, &conn), 6);
  EXPECT_UPDATE(Execute(update2, &conn), 1);
  EXPECT_UPDATE(Execute(update3, &conn), 2);

  // Validate insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("addr", SN("user", "0")), (V{"|1|0|30|"}));
  EXPECT_EQ(db->GetShard("addr", SN("user", "5")),
            (V{"|0|5|20|", "|2|5|400|"}));
  EXPECT_EQ(db->GetShard("addr", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetShard("phones", SN("user", "0")), (V{"|0|1|", "|1|1|"}));
  EXPECT_EQ(db->GetShard("phones", SN("user", "5")), (V{"|2|0|", "|3|0|"}));
  EXPECT_EQ(db->GetShard("phones", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();

  // Some errors.
  auto err1 = MakeUpdate("phones", {{"aid", "10"}}, {"id = 0"});
  auto err2 = MakeUpdate("addr", {{"uid", "100"}}, {"id = 0"});
  EXPECT_TRUE(ExecuteError(err1, &conn));
  EXPECT_TRUE(ExecuteError(err2, &conn));
}

TEST_F(UpdateTest, TwoOwners) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "user(id)", "receiver" I OB "user(id)"});

  // Make a pelton connection.
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

  // Update an OB field of a record which has two owners
  //    (the record should be moved from one shard and stay in the other one).
  auto update1 = MakeUpdate("msg", {{"sender", "5"}}, {"id = 1"});
  auto update2 = MakeUpdate("msg", {{"sender", "sender + 5"}}, {"id = 2"});
  auto update3 = MakeUpdate("msg", {{"receiver", "5"}}, {"id = 4"});
  EXPECT_UPDATE(Execute(update1, &conn), 4);
  EXPECT_UPDATE(Execute(update2, &conn), 3);
  EXPECT_UPDATE(Execute(update3, &conn), 3);

  // Validate insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("msg", SN("user", "0")), (V{"|2|5|0|"}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "5")),
            (V{"|1|5|10|", "|2|5|0|", "|3|5|10|", "|4|5|5|"}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "10")), (V{"|1|5|10|", "|3|5|10|"}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();

  // Errors.
  auto err1 = MakeUpdate("msg", {{"sender", "50"}}, {"id = 1"});
  auto err2 = MakeUpdate("msg", {{"receiver", "50"}}, {"id = 1"});
  EXPECT_TRUE(ExecuteError(err1, &conn));
  EXPECT_TRUE(ExecuteError(err2, &conn));
}

TEST_F(UpdateTest, VariableOwnership) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string grps = MakeCreate("grps", {"gid" I PK});
  std::string assoc = MakeCreate(
      "association",
      {"id" I PK, "group_id" I OW "grps(gid)", "user_id" I OB "user(id)"});

  // Make a pelton connection.
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
  auto &&[grps3, row3] = MakeInsert("grps", {"2"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(grps1, &conn), 1);
  EXPECT_UPDATE(Execute(grps2, &conn), 1);
  EXPECT_UPDATE(Execute(grps3, &conn), 1);

  // Validate insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)),
            (V{row1, row2, row3}));
  db->RollbackTransaction();

  // Associate groups with some users.
  auto &&[assoc1, a_] = MakeInsert("association", {"0", "0", "0"});
  auto &&[assoc2, a__] = MakeInsert("association", {"1", "1", "0"});
  auto &&[assoc3, a___] = MakeInsert("association", {"2", "1", "5"});
  auto &&[assoc4, a____] = MakeInsert("association", {"3", "1", "5"});

  EXPECT_UPDATE(Execute(assoc1, &conn), 3);
  EXPECT_UPDATE(Execute(assoc2, &conn), 3);
  EXPECT_UPDATE(Execute(assoc3, &conn), 2);
  EXPECT_UPDATE(Execute(assoc4, &conn), 1);

  // Update an OWNED BY field of a record.
  auto update1 = MakeUpdate("association", {{"user_id", "5"}}, {"id = 0"});
  EXPECT_UPDATE(Execute(update1, &conn), 4);

  // Validate moves after insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("association", SN("user", "0")), (V{"|1|1|0|"}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{"|1|"}));
  EXPECT_EQ(db->GetShard("association", SN("user", "5")),
            (V{"|0|0|5|", "|2|1|5|", "|3|1|5|"}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{"|0|", "|1|"}));
  EXPECT_EQ(db->GetShard("association", SN(DEFAULT_SHARD, DEFAULT_SHARD)),
            (V{}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{"|2|"}));
  db->RollbackTransaction();

  // Update an OWNS field of a record.
  auto update2 = MakeUpdate("association", {{"group_id", "0"}}, {"id = 3"});
  EXPECT_UPDATE(Execute(update2, &conn), 1);

  // Validate moves after insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("association", SN("user", "0")), (V{"|1|1|0|"}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{"|1|"}));
  EXPECT_EQ(db->GetShard("association", SN("user", "5")),
            (V{"|0|0|5|", "|2|1|5|", "|3|0|5|"}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{"|0|", "|1|"}));
  EXPECT_EQ(db->GetShard("association", SN(DEFAULT_SHARD, DEFAULT_SHARD)),
            (V{}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{"|2|"}));
  db->RollbackTransaction();

  // Update an OWNS field of a record.
  auto update3 = MakeUpdate("association", {{"group_id", "2"}}, {"id = 2"});
  EXPECT_UPDATE(Execute(update3, &conn), 4);

  // Validate moves after insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("association", SN("user", "0")), (V{"|1|1|0|"}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{"|1|"}));
  EXPECT_EQ(db->GetShard("association", SN("user", "5")),
            (V{"|0|0|5|", "|2|2|5|", "|3|0|5|"}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{"|0|", "|2|"}));
  EXPECT_EQ(db->GetShard("association", SN(DEFAULT_SHARD, DEFAULT_SHARD)),
            (V{}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();

  // Update to non-existing OWNS or OWNED BY.
  auto update4 = MakeUpdate("association", {{"group_id", "20"}}, {"id = 2"});
  auto update5 = MakeUpdate("association", {{"user_id", "50"}}, {"id = 2"});
  EXPECT_TRUE(ExecuteError(update4, &conn));
  EXPECT_TRUE(ExecuteError(update5, &conn));
}

TEST_F(UpdateTest, ComplexVariableOwnership) {
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

  // Make a pelton connection.
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

  // Associate groups and users.
  auto &&[assoc1, a_] = MakeInsert("association", {"0", "1", "5"});
  auto &&[reassoc1, ra_] = MakeInsert("association", {"1", "1", "5"});
  auto &&[reassoc2, ra__] = MakeInsert("association", {"2", "1", "0"});
  EXPECT_UPDATE(Execute(assoc1, &conn), 6);
  EXPECT_UPDATE(Execute(reassoc1, &conn), 1);
  EXPECT_UPDATE(Execute(reassoc2, &conn), 4);

  // Some updates.
  auto update1 = MakeUpdate("association", {{"user_id", "5"}}, {"sid = 2"});
  auto update2 = MakeUpdate("fassoc", {{"file", "1"}}, {"file = 0"});
  auto update3 =
      MakeUpdate("association", {{"user_id", "0"}}, {"sid IN (0, 1)"});
  auto update4 = MakeUpdate("association", {{"user_id", "0"}}, {"sid = 2"});
  auto update5 = MakeUpdate("fassoc", {{"file", "0"}}, {"fsid = 0"});
  EXPECT_UPDATE(Execute(update1, &conn), 5);
  EXPECT_UPDATE(Execute(update2, &conn), 4);
  EXPECT_UPDATE(Execute(update3, &conn), 7);
  EXPECT_UPDATE(Execute(update4, &conn), 6);
  EXPECT_UPDATE(Execute(update5, &conn), 3);

  // Validate moves after insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("grps", SN("admin", "0")), (V{"|0|0|", "|1|0|"}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{"|1|0|"}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));

  EXPECT_EQ(db->GetShard("association", SN("admin", "0")), (V{}));
  EXPECT_EQ(db->GetShard("association", SN("user", "0")),
            (V{"|0|1|0|", "|1|1|0|", "|2|1|0|"}));
  EXPECT_EQ(db->GetShard("association", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("association", SN(DEFAULT_SHARD, DEFAULT_SHARD)),
            (V{}));

  EXPECT_EQ(db->GetShard("fassoc", SN("admin", "0")),
            (V{"|0|0|1|", "|1|1|1|"}));
  EXPECT_EQ(db->GetShard("fassoc", SN("user", "0")), (V{"|0|0|1|", "|1|1|1|"}));
  EXPECT_EQ(db->GetShard("fassoc", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));

  EXPECT_EQ(db->GetShard("files", SN("admin", "0")), (V{"|0|0|", "|1|0|"}));
  EXPECT_EQ(db->GetShard("files", SN("user", "0")), (V{"|0|0|", "|1|0|"}));
  EXPECT_EQ(db->GetShard("files", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("files", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();

  // FK integrity violations.
  auto err1 = MakeUpdate("fassoc", {{"file", "3"}}, {"fsid = 0"});
  auto err2 = MakeUpdate("fassoc", {{"group_id", "3"}}, {"fsid = 0"});
  auto err3 = MakeUpdate("association", {{"user_id", "3"}}, {"sid = 0"});
  auto err4 = MakeUpdate("association", {{"group_id", "3"}}, {"sid = 0"});
  auto err5 = MakeUpdate("files", {{"creator", "3"}}, {"fid = 0"});
  auto err6 = MakeUpdate("grps", {{"admin", "3"}}, {"gid = 0"});
  EXPECT_TRUE(ExecuteError(err1, &conn));
  EXPECT_TRUE(ExecuteError(err2, &conn));
  EXPECT_TRUE(ExecuteError(err3, &conn));
  EXPECT_TRUE(ExecuteError(err4, &conn));
  EXPECT_TRUE(ExecuteError(err5, &conn));
  EXPECT_TRUE(ExecuteError(err6, &conn));
}

TEST_F(UpdateTest, ReplaceTest) {
  // Parse create table statements.
  std::string data = MakeCreate("data", {"id" I PK, "data" I});
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string t1 = MakeCreate("t1", {"id" I PK, "uid" I OB "user(id)", "d" I});

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(data, &conn));
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(t1, &conn));

  // Perform some inserts.
  auto &&[usr1, _] = MakeReplace("user", {"0", "'u1'"});
  auto &&[usr2, __] = MakeReplace("user", {"5", "'u10'"});
  auto &&[t11, ___] = MakeReplace("t1", {"0", "0", "100"});
  auto &&[t12, ____] = MakeReplace("t1", {"1", "0", "200"});
  auto &&[un1, _____] = MakeReplace("data", {"0", "500"});
  auto &&[un2, ______] = MakeReplace("data", {"1", "600"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(t11, &conn), 1);
  EXPECT_UPDATE(Execute(t12, &conn), 1);
  EXPECT_UPDATE(Execute(un1, &conn), 1);
  EXPECT_UPDATE(Execute(un2, &conn), 1);

  // Replace existing rows.
  auto &&[rep1, x] = MakeReplace("data", {"1", "2000"});
  auto &&[rep2, xx] = MakeReplace("user", {"0", "'u7'"});
  auto &&[rep3, xxx] = MakeReplace("t1", {"1", "0", "9000"});
  auto &&[rep4, xxxx] = MakeReplace("t1", {"0", "5", "666"});

  EXPECT_UPDATE(Execute(rep1, &conn), 1);
  EXPECT_UPDATE(Execute(rep2, &conn), 1);
  EXPECT_UPDATE(Execute(rep3, &conn), 2);
  EXPECT_UPDATE(Execute(rep4, &conn), 2);

  // Validate replaces.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("data", SN(DEFAULT_SHARD, DEFAULT_SHARD)),
            (V{"|0|500|", "|1|2000|"}));

  EXPECT_EQ(db->GetShard("user", SN("user", "0")), (V{"|0|u7|"}));
  EXPECT_EQ(db->GetShard("user", SN("user", "5")), (V{"|5|u10|"}));
  EXPECT_EQ(db->GetShard("user", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));

  EXPECT_EQ(db->GetShard("t1", SN("user", "0")), (V{"|1|0|9000|"}));
  EXPECT_EQ(db->GetShard("t1", SN("user", "5")), (V{"|0|5|666|"}));
  EXPECT_EQ(db->GetShard("t1", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
