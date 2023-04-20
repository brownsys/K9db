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

  // Perform some inserts.
  auto &&[stmt1, row1] = MakeInsert("user", {"0", "'u1'"});
  auto &&[stmt2, row2] = MakeInsert("user", {"1", "'u2'"});
  auto &&[stmt3, row3] = MakeInsert("user", {"2", "'u3'"});
  auto &&[stmt4, row4] = MakeInsert("user", {"5", "'u10'"});

  // Update one field.
  auto &&update_stmt1 = MakeUpdate("user", {{"name", "'u5'"}}, {{"id", "0"}});
  // Update two records to have the same field.
  auto &&update_stmt2 = MakeUpdate("user", {{"name", "'u7'"}}, {{"id", "1"}});
  auto &&update_stmt3 = MakeUpdate("user", {{"name", "'u7'"}}, {{"id", "2"}});
  // Update two values in one statement.
  auto &&update_stmt4 =
      MakeUpdate("user", {{"name", "'u8'"}}, {{"name", "'u7'"}});

  EXPECT_UPDATE(Execute(stmt1, &conn), 1);
  EXPECT_UPDATE(Execute(stmt2, &conn), 1);
  EXPECT_UPDATE(Execute(stmt3, &conn), 1);
  EXPECT_UPDATE(Execute(stmt4, &conn), 1);

  // Perform the updates.
  Execute(update_stmt1, &conn);
  Execute(update_stmt2, &conn);
  Execute(update_stmt3, &conn);
  Execute(update_stmt4, &conn);

  // Validate insertions and updates.
  db->BeginTransaction(false);
  sql::SqlResultSet r = db->GetShard("user", SN(DEFAULT_SHARD, DEFAULT_SHARD));
  EXPECT_EQ(r, (V{"|0|u5|", "|1|u8|", "|2|u8|", row4}));
  db->RollbackTransaction();
}

TEST_F(UpdateTest, Datasubject) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));

  // Perform some inserts.
  auto &&[stmt1, row1] = MakeInsert("user", {"0", "'u1'"});
  auto &&[stmt2, row2] = MakeInsert("user", {"1", "'u2'"});
  auto &&[stmt3, row3] = MakeInsert("user", {"2", "'u3'"});
  auto &&[stmt4, row4] = MakeInsert("user", {"5", "'u10'"});

  // Update one field.
  auto &&update_stmt1 = MakeUpdate("user", {{"name", "'u5'"}}, {{"id", "0"}});
  // Update two records to have the same field.
  auto &&update_stmt2 = MakeUpdate("user", {{"name", "'u7'"}}, {{"id", "1"}});
  auto &&update_stmt3 = MakeUpdate("user", {{"name", "'u7'"}}, {{"id", "2"}});
  // Update two values in one statement.
  auto &&update_stmt4 =
      MakeUpdate("user", {{"name", "'u8'"}}, {{"name", "'u7'"}});

  EXPECT_UPDATE(Execute(stmt1, &conn), 1);
  EXPECT_UPDATE(Execute(stmt2, &conn), 1);
  EXPECT_UPDATE(Execute(stmt3, &conn), 1);
  EXPECT_UPDATE(Execute(stmt4, &conn), 1);

  // Perform the updates.
  Execute(update_stmt1, &conn);
  Execute(update_stmt2, &conn);
  Execute(update_stmt3, &conn);
  Execute(update_stmt4, &conn);

  // Validate insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("user", SN("user", "0")), (V{"|0|u5|"}));
  EXPECT_EQ(db->GetShard("user", SN("user", "1")), (V{"|1|u8|"}));
  EXPECT_EQ(db->GetShard("user", SN("user", "2")), (V{"|2|u8|"}));
  EXPECT_EQ(db->GetShard("user", SN("user", "5")), (V{row4}));
  EXPECT_EQ(db->GetShard("user", SN("user", "10")), (V{}));
  EXPECT_EQ(db->GetShard("user", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(UpdateTest, DirectTable) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string addr = MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(addr, &conn));

  // Perform some inserts.
  auto &&[usr1, _] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, __] = MakeInsert("user", {"5", "'u10'"});
  auto &&[addr1, row1] = MakeInsert("addr", {"0", "0"});
  auto &&[addr2, row2] = MakeInsert("addr", {"1", "0"});
  auto &&[addr3, row3] = MakeInsert("addr", {"2", "5"});

  // Update FK in a directly sharded table (should move to a different shard).
  auto &&update_stmt = MakeUpdate("addr", {{"uid", "5"}}, {{"id", "0"}});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr1, &conn), 1);
  EXPECT_UPDATE(Execute(addr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr3, &conn), 1);

  // Perform the update.
  Execute(update_stmt, &conn);

  // Validate insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("addr", SN("user", "0")), (V{row2}));
  EXPECT_EQ(db->GetShard("addr", SN("user", "5")), (V{row3, "|0|5|"}));
  EXPECT_EQ(db->GetShard("addr", SN("user", "10")), (V{}));
  EXPECT_EQ(db->GetShard("addr", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(UpdateTest, TransitiveTable) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string addr = MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});
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
  auto &&[addr1, ___] = MakeInsert("addr", {"0", "0"});
  auto &&[addr2, ____] = MakeInsert("addr", {"1", "0"});
  auto &&[addr3, _____] = MakeInsert("addr", {"2", "5"});
  auto &&[num1, row1] = MakeInsert("phones", {"0", "2"});
  auto &&[num2, row2] = MakeInsert("phones", {"1", "1"});
  auto &&[num3, row3] = MakeInsert("phones", {"2", "0"});
  auto &&[num4, row4] = MakeInsert("phones", {"3", "0"});

  // Update FK in a transitively sharded table
  //    (should move transitively owned records to a different shard too).
  auto &&update_stmt = MakeUpdate("addr", {{"uid", "5"}}, {{"id", "0"}});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr1, &conn), 1);
  EXPECT_UPDATE(Execute(addr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr3, &conn), 1);
  EXPECT_UPDATE(Execute(num1, &conn), 1);
  EXPECT_UPDATE(Execute(num2, &conn), 1);
  EXPECT_UPDATE(Execute(num3, &conn), 1);
  EXPECT_UPDATE(Execute(num4, &conn), 1);

  // Perform the update.
  Execute(update_stmt, &conn);

  // Validate insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("phones", SN("user", "0")), (V{row2}));
  EXPECT_EQ(db->GetShard("phones", SN("user", "5")), (V{row1, row3, row4}));
  EXPECT_EQ(db->GetShard("phones", SN("user", "10")), (V{}));
  EXPECT_EQ(db->GetShard("phones", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(UpdateTest, TransitiveTableShardingError) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string addr = MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});
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
  auto &&[addr1, ___] = MakeInsert("addr", {"0", "0"});
  auto &&[addr2, ____] = MakeInsert("addr", {"1", "0"});
  auto &&[addr3, _____] = MakeInsert("addr", {"2", "5"});
  auto &&[num1, row1] = MakeInsert("phones", {"0", "2"});
  auto &&[num2, row2] = MakeInsert("phones", {"1", "1"});
  auto &&[num3, row3] = MakeInsert("phones", {"2", "0"});
  auto &&[num4, row4] = MakeInsert("phones", {"3", "0"});

  // Update FK to a non-existing value in a transitively sharded table
  //    (should throw an error and not perform any moves).
  auto &&update_stmt = MakeUpdate("addr", {{"uid", "20"}}, {{"id", "0"}});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr1, &conn), 1);
  EXPECT_UPDATE(Execute(addr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr3, &conn), 1);
  EXPECT_UPDATE(Execute(num1, &conn), 1);
  EXPECT_UPDATE(Execute(num2, &conn), 1);
  EXPECT_UPDATE(Execute(num3, &conn), 1);
  EXPECT_UPDATE(Execute(num4, &conn), 1);

  // Perform the update.
  absl::StatusOr<sql::SqlResult> result = Shard(update_stmt, &conn);
  EXPECT_FALSE(result.ok());

  // Validate insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("phones", SN("user", "0")), (V{row2, row3, row4}));
  EXPECT_EQ(db->GetShard("phones", SN("user", "5")), (V{row1}));
  EXPECT_EQ(db->GetShard("phones", SN("user", "10")), (V{}));
  EXPECT_EQ(db->GetShard("phones", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
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

  // Update an OB field of a record which has two owners
  //    (the record should be moved from one shard and stay in the other one).
  auto &&update_stmt = MakeUpdate("msg", {{"sender", "5"}}, {{"id", "1"}});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 2);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);  // Inserted only once for u1.
  EXPECT_UPDATE(Execute(msg3, &conn), 2);
  EXPECT_UPDATE(Execute(msg4, &conn), 2);

  // Perform the update.
  Execute(update_stmt, &conn);

  // Validate insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("msg", SN("user", "0")), (V{row2, row4}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "5")), (V{"|1|5|10|", row3, row4}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "10")), (V{"|1|5|10|", row3}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(UpdateTest, OwnerAccessor) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "user(id)", "receiver" I AB "user(id)"});

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

  // Update an ACCESSED BY field of a record (the record should not be moved).
  auto &&update_stmt = MakeUpdate("msg", {{"receiver", "5"}}, {{"id", "1"}});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 1);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);
  EXPECT_UPDATE(Execute(msg3, &conn), 1);
  EXPECT_UPDATE(Execute(msg4, &conn), 1);

  // Perform the update.
  Execute(update_stmt, &conn);

  // Validate insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("msg", SN("user", "0")), (V{"|1|0|5|", row2}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "5")), (V{row3, row4}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "10")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
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

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(grps1, &conn), 1);
  EXPECT_UPDATE(Execute(grps2, &conn), 1);

  // Validate insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)),
            (V{row1, row2}));
  db->RollbackTransaction();

  // Associate groups with some users.
  auto &&[assoc1, a_] = MakeInsert("association", {"0", "0", "0"});
  auto &&[assoc2, a__] = MakeInsert("association", {"1", "1", "0"});
  auto &&[assoc3, a___] = MakeInsert("association", {"2", "1", "5"});
  auto &&[assoc4, a____] = MakeInsert("association", {"3", "1", "5"});

  // Update an OWNED BY field of a record.
  auto &&update_stmt1 =
      MakeUpdate("association", {{"user_id", "5"}}, {{"id", "0"}});

  EXPECT_UPDATE(Execute(assoc1, &conn), 3);
  EXPECT_UPDATE(Execute(assoc2, &conn), 3);
  EXPECT_UPDATE(Execute(assoc3, &conn), 2);
  EXPECT_UPDATE(Execute(assoc4, &conn), 1);

  // Perform the update.
  Execute(update_stmt1, &conn);

  // Validate moves after insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{row2}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{row1, row2}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();

  // Update an OWNS field of a record.
  auto &&update_stmt2 =
      MakeUpdate("association", {{"group_id", "0"}}, {{"id", "1"}});

  // Perform another update.
  Execute(update_stmt2, &conn);

  // Validate moves after another update.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{row1}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{row1, row2}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(UpdateTest, VariableOwnershipShardingError) {
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

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(grps1, &conn), 1);
  EXPECT_UPDATE(Execute(grps2, &conn), 1);

  // Validate insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)),
            (V{row1, row2}));
  db->RollbackTransaction();

  // Associate groups with some users.
  auto &&[assoc1, a_] = MakeInsert("association", {"0", "0", "0"});
  auto &&[assoc2, a__] = MakeInsert("association", {"1", "1", "0"});
  auto &&[assoc3, a___] = MakeInsert("association", {"2", "1", "5"});
  auto &&[assoc4, a____] = MakeInsert("association", {"3", "1", "5"});

  // Update an OWNED BY field of a record to point to a non-existing user.
  auto &&update_stmt1 =
      MakeUpdate("association", {{"user_id", "7"}}, {{"id", "0"}});

  EXPECT_UPDATE(Execute(assoc1, &conn), 3);
  EXPECT_UPDATE(Execute(assoc2, &conn), 3);
  EXPECT_UPDATE(Execute(assoc3, &conn), 2);
  EXPECT_UPDATE(Execute(assoc4, &conn), 1);

  // Perform the update.
  absl::StatusOr<sql::SqlResult> result1 = Shard(update_stmt1, &conn);
  EXPECT_FALSE(result1.ok());

  // Validate moves after insertions and updates.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{row1, row2}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{row2}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();

  // Update an OWNS field of a record.
  auto &&update_stmt2 =
      MakeUpdate("association", {{"group_id", "7"}}, {{"id", "0"}});

  // Perform another update.
  absl::StatusOr<sql::SqlResult> result2 = Shard(update_stmt2, &conn);
  EXPECT_FALSE(result2.ok());

  // Validate moves after another update.
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{row1, row2}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{row1}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
