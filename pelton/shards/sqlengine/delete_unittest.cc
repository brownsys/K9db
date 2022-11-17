#include "pelton/shards/sqlengine/delete.h"

#include <string>
#include <unordered_set>
#include <vector>

#include "glog/logging.h"
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
PELTON_FIXTURE(DeleteTest);

TEST_F(DeleteTest, UnshardedTable) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR});

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::AbstractConnection *db = conn.state->Database();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));

  // Perform some inserts.
  auto &&[stmt1, row1] = MakeInsert("user", {"0", "'u1'"});
  auto &&[stmt2, row2] = MakeInsert("user", {"1", "'u2'"});
  auto &&[stmt3, row3] = MakeInsert("user", {"2", "'u10'"});
  auto &&[stmt4, row4] = MakeInsert("user", {"5", "'u10'"});

  EXPECT_UPDATE(Execute(stmt1, &conn), 1);
  EXPECT_UPDATE(Execute(stmt2, &conn), 1);
  EXPECT_UPDATE(Execute(stmt3, &conn), 1);
  EXPECT_UPDATE(Execute(stmt4, &conn), 1);

  // Do some deletes.
  auto del1 = MakeDelete("user", {"id = 1"});
  auto del2 = MakeDelete("user", {"name = 'u10'"});
  auto del3 = MakeDelete("user", {"id = 0", "name = 'u33'"});

  EXPECT_UPDATE(Execute(del1, &conn), 1);
  EXPECT_UPDATE(Execute(del2, &conn), 2);
  EXPECT_UPDATE(Execute(del3, &conn), 0);

  // Validate deletion.
  EXPECT_EQ(db->GetAll("user"), (V{row1}));
}

TEST_F(DeleteTest, DirectTable) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string addr = MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::AbstractConnection *db = conn.state->Database();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(addr, &conn));

  // Perform some inserts.
  auto &&[usr1, _] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, __] = MakeInsert("user", {"5", "'u10'"});
  auto &&[addr1, row1] = MakeInsert("addr", {"0", "0"});
  auto &&[addr2, row2] = MakeInsert("addr", {"1", "0"});
  auto &&[addr3, row3] = MakeInsert("addr", {"2", "5"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr1, &conn), 1);
  EXPECT_UPDATE(Execute(addr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr3, &conn), 1);

  auto del1 = MakeDelete("addr", {"uid = 0"});
  auto del2 = MakeDelete("addr", {"id = 2"});

  EXPECT_UPDATE(Execute(del1, &conn), 2);
  EXPECT_UPDATE(Execute(del2, &conn), 1);

  // Validate insertions.
  EXPECT_EQ(db->GetAll("addr"), (V{}));
}

TEST_F(DeleteTest, DeepTransitiveTable) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string addr = MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});
  std::string nums = MakeCreate("phones", {"id" I PK, "aid" I FK "addr(id)"});
  std::string deep = MakeCreate("deep", {"id" I PK, "pid" I FK "phones(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::AbstractConnection *db = conn.state->Database();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(addr, &conn));
  EXPECT_SUCCESS(Execute(nums, &conn));
  EXPECT_SUCCESS(Execute(deep, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u5] = MakeInsert("user", {"5", "'u10'"});
  auto &&[addr1, a0] = MakeInsert("addr", {"0", "0"});
  auto &&[addr2, a1] = MakeInsert("addr", {"1", "0"});
  auto &&[addr3, a2] = MakeInsert("addr", {"2", "5"});
  auto &&[num1, p0] = MakeInsert("phones", {"0", "2"});
  auto &&[num2, p1] = MakeInsert("phones", {"1", "1"});
  auto &&[num3, p2] = MakeInsert("phones", {"2", "0"});
  auto &&[num4, p3] = MakeInsert("phones", {"3", "0"});
  auto &&[deep1, d0] = MakeInsert("deep", {"0", "2"});
  auto &&[deep2, d1] = MakeInsert("deep", {"1", "2"});
  auto &&[deep3, d2] = MakeInsert("deep", {"2", "0"});
  auto &&[deep4, d3] = MakeInsert("deep", {"3", "1"});
  auto &&[deep5, d4] = MakeInsert("deep", {"4", "3"});

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

  // Do some deletes.
  auto del1 = MakeDelete("deep", {"id = 3"});
  auto del2 = MakeDelete("phones", {"id = 3"});
  auto del3 = MakeDelete("addr", {"id IN (0, 1)"});

  EXPECT_UPDATE(Execute(del1, &conn), 1);
  EXPECT_UPDATE(Execute(del2, &conn), 3);
  EXPECT_UPDATE(Execute(del3, &conn), 10);

  // Validate deletion.
  EXPECT_EQ(db->GetShard("deep", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("deep", SN("user", "5")), (V{d2}));
  EXPECT_EQ(db->GetShard("deep", SN(DEFAULT_SHARD, DEFAULT_SHARD)),
            (V{d0, d1, d4}));
  EXPECT_EQ(db->GetAll("deep"), (V{d0, d1, d2, d4}));

  EXPECT_EQ(db->GetShard("phones", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("phones", SN("user", "5")), (V{p0}));
  EXPECT_EQ(db->GetShard("phones", SN(DEFAULT_SHARD, DEFAULT_SHARD)),
            (V{p1, p2}));
  EXPECT_EQ(db->GetAll("phones"), (V{p0, p1, p2}));

  EXPECT_EQ(db->GetShard("addr", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("addr", SN("user", "5")), (V{a2}));
  EXPECT_EQ(db->GetShard("addr", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetAll("addr"), (V{a2}));

  EXPECT_EQ(db->GetShard("user", SN("user", "0")), (V{u0}));
  EXPECT_EQ(db->GetShard("user", SN("user", "5")), (V{u5}));
  EXPECT_EQ(db->GetShard("user", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetAll("user"), (V{u0, u5}));
}

TEST_F(DeleteTest, TwoOwners) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "user(id)", "receiver" I OB "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::AbstractConnection *db = conn.state->Database();

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
  auto &&[msg5, row5] = MakeInsert("msg", {"5", "0", "5"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 2);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);  // Inserted only once for u1.
  EXPECT_UPDATE(Execute(msg3, &conn), 2);
  EXPECT_UPDATE(Execute(msg4, &conn), 2);
  EXPECT_UPDATE(Execute(msg5, &conn), 2);

  // Do some deletes.
  auto del1 = MakeDelete("msg", {"id = 1"});
  auto del2 = MakeDelete("msg", {"receiver = 0"});

  EXPECT_UPDATE(Execute(del1, &conn), 2);
  EXPECT_UPDATE(Execute(del2, &conn), 3);

  // Validate deletion.
  EXPECT_EQ(db->GetShard("msg", SN("user", "0")), (V{row5}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "5")), (V{row3, row5}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "10")), (V{row3}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetAll("msg"), (V{row3, row5}));
}

TEST_F(DeleteTest, ShardedDataSubject) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string middle = MakeCreate("mid", {"id" I PK, "uid" I FK "user(id)"});
  std::string invited = MakeCreate(
      "invited", {"id" I PK, "name" STR, "inviting_mid" I FK "mid(id)"}, true);
  std::string attr = MakeCreate("attr", {"id" I PK, "iid" I FK "invited(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::AbstractConnection *db = conn.state->Database();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(middle, &conn));
  EXPECT_SUCCESS(Execute(invited, &conn));
  EXPECT_SUCCESS(Execute(attr, &conn));

  // Perform some inserts.
  auto &&[usr1, u_] = MakeInsert("user", {"0", "'u1'"});
  auto &&[mid1, rm1] = MakeInsert("mid", {"10", "0"});
  auto &&[inv1, row1] = MakeInsert("invited", {"100", "'inv1'", "10"});
  auto &&[inv2, row2] = MakeInsert("invited", {"500", "'inv2'", "10"});
  auto &&[attr1, ra1] = MakeInsert("attr", {"2", "100"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(mid1, &conn), 1);
  EXPECT_UPDATE(Execute(inv1, &conn), 2);
  EXPECT_UPDATE(Execute(inv2, &conn), 2);
  EXPECT_UPDATE(Execute(attr1, &conn), 2);

  // Do some deletes.
  auto del1 = MakeDelete("mid", {"id = 10"});
  auto del2 = MakeDelete("invited", {"id = 500"});

  EXPECT_UPDATE(Execute(del1, &conn), 4);
  EXPECT_UPDATE(Execute(del2, &conn), 1);

  // Validate insertions.
  EXPECT_EQ(db->GetAll("mid"), (V{}));

  EXPECT_EQ(db->GetShard("invited", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("invited", SN("invited", "100")), (V{row1}));
  EXPECT_EQ(db->GetShard("invited", SN("invited", "500")), (V{}));
  EXPECT_EQ(db->GetShard("invited", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetAll("invited"), (V{row1}));

  EXPECT_EQ(db->GetShard("attr", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("attr", SN("invited", "100")), (V{ra1}));
  EXPECT_EQ(db->GetShard("attr", SN("invited", "500")), (V{}));
  EXPECT_EQ(db->GetShard("attr", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetAll("attr"), (V{ra1}));
}

TEST_F(DeleteTest, VariableOwnership) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string grps = MakeCreate("grps", {"gid" I PK});
  std::string assoc = MakeCreate(
      "association",
      {"id" I PK, "group_id" I OW "grps(gid)", "user_id" I OB "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::AbstractConnection *db = conn.state->Database();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(grps, &conn));
  EXPECT_SUCCESS(Execute(assoc, &conn));

  // Perform some inserts.
  auto &&[usr1, u_] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u__] = MakeInsert("user", {"5", "'u10'"});
  auto &&[grps1, row1] = MakeInsert("grps", {"0"});
  auto &&[assoc1, a_] = MakeInsert("association", {"50", "0", "0"});
  auto &&[assoc2, a__] = MakeInsert("association", {"100", "0", "0"});
  auto &&[assoc3, a___] = MakeInsert("association", {"10", "0", "5"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(grps1, &conn), 1);
  EXPECT_UPDATE(Execute(assoc1, &conn), 3);
  EXPECT_UPDATE(Execute(assoc2, &conn), 1);
  EXPECT_UPDATE(Execute(assoc3, &conn), 2);

  // Do some deletes.
  auto del1 = MakeDelete("association", {"id = 50"});
  auto del2 = MakeDelete("association", {"id = 100"});
  auto del3 = MakeDelete("association", {"id = 10"});

  // Delete and validate.
  EXPECT_UPDATE(Execute(del1, &conn), 1);
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{row1}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{row1}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetAll("grps"), (V{row1}));

  EXPECT_UPDATE(Execute(del2, &conn), 2);
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{row1}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetAll("grps"), (V{row1}));

  EXPECT_UPDATE(Execute(del3, &conn), 3);
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{row1}));
  EXPECT_EQ(db->GetAll("grps"), (V{row1}));
}

TEST_F(DeleteTest, ComplexVariableOwnership) {
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
  sql::AbstractConnection *db = conn.state->Database();

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
  auto &&[grps1, grow1] = MakeInsert("grps", {"10", "0"});
  auto &&[grps2, grow2] = MakeInsert("grps", {"15", "0"});
  auto &&[assoc1, a_] = MakeInsert("association", {"100", "10", "0"});
  auto &&[assoc2, a__] = MakeInsert("association", {"200", "10", "5"});
  auto &&[assoc3, a___] = MakeInsert("association", {"300", "15", "0"});
  auto &&[assoc4, a____] = MakeInsert("association", {"300", "15", "5"});
  auto &&[f1, frow1] = MakeInsert("files", {"55", "0"});
  auto &&[f2, frow2] = MakeInsert("files", {"56", "0"});
  auto &&[fa1, farow1] = MakeInsert("fassoc", {"20", "55", "10"});
  auto &&[fa2, farow2] = MakeInsert("fassoc", {"21", "56", "10"});
  auto &&[fa3, farow3] = MakeInsert("fassoc", {"22", "56", "15"});
  auto &&[fa4, farow4] = MakeInsert("fassoc", {"23", "56", "15"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(adm1, &conn), 1);
  EXPECT_UPDATE(Execute(grps1, &conn), 1);
  EXPECT_UPDATE(Execute(assoc1, &conn), 2);
  EXPECT_UPDATE(Execute(assoc2, &conn), 2);
  EXPECT_UPDATE(Execute(assoc3, &conn), 1);
  EXPECT_UPDATE(Execute(assoc4, &conn), 1);
  EXPECT_UPDATE(Execute(grps2, &conn), 3);
  EXPECT_UPDATE(Execute(f1, &conn), 1);
  EXPECT_UPDATE(Execute(f2, &conn), 1);
  EXPECT_UPDATE(Execute(fa1, &conn), 5);
  EXPECT_UPDATE(Execute(fa2, &conn), 5);
  EXPECT_UPDATE(Execute(fa3, &conn), 3);
  EXPECT_UPDATE(Execute(fa4, &conn), 3);

  // Delete from fassoc and validate.
  auto del1 = MakeDelete("fassoc", {"fsid = 21"});
  EXPECT_UPDATE(Execute(del1, &conn), 3);
  EXPECT_EQ(db->GetShard("files", SN("user", "0")), (V{frow1, frow2}));
  EXPECT_EQ(db->GetShard("files", SN("user", "5")), (V{frow1, frow2}));
  EXPECT_EQ(db->GetShard("files", SN("admin", "0")), (V{frow1, frow2}));
  EXPECT_EQ(db->GetShard("files", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetAll("files"), (V{frow1, frow2}));

  // Delete from group and validate.
  auto del2 = MakeDelete("grps", {"gid = 15"});
  EXPECT_UPDATE(Execute(del2, &conn), 13);
  EXPECT_EQ(db->GetShard("files", SN("user", "0")), (V{frow1, frow2}));
  EXPECT_EQ(db->GetShard("files", SN("user", "5")), (V{frow1}));
  EXPECT_EQ(db->GetShard("files", SN("admin", "0")), (V{frow1}));
  EXPECT_EQ(db->GetShard("files", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetAll("files"), (V{frow1, frow2}));

  EXPECT_EQ(db->GetShard("fassoc", SN("user", "0")), (V{farow1}));
  EXPECT_EQ(db->GetShard("fassoc", SN("user", "5")), (V{farow1}));
  EXPECT_EQ(db->GetShard("fassoc", SN("admin", "0")), (V{farow1}));
  EXPECT_EQ(db->GetShard("fassoc", SN(DEFAULT_SHARD, DEFAULT_SHARD)),
            (V{farow3, farow4}));
  EXPECT_EQ(db->GetAll("fassoc"), (V{farow1, farow3, farow4}));

  // Delete from association and validate.
  auto del3 = MakeDelete("association", {"group_id = 10"});
  EXPECT_UPDATE(Execute(del3, &conn), 7);
  EXPECT_EQ(db->GetShard("files", SN("user", "0")), (V{frow1, frow2}));
  EXPECT_EQ(db->GetShard("files", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("files", SN("admin", "0")), (V{frow1}));
  EXPECT_EQ(db->GetShard("files", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetAll("files"), (V{frow1, frow2}));

  EXPECT_EQ(db->GetShard("fassoc", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN("admin", "0")), (V{farow1}));
  EXPECT_EQ(db->GetShard("fassoc", SN(DEFAULT_SHARD, DEFAULT_SHARD)),
            (V{farow3, farow4}));
  EXPECT_EQ(db->GetAll("fassoc"), (V{farow1, farow3, farow4}));

  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("admin", "0")), (V{grow1}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetAll("grps"), (V{grow1}));
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton