#include "pelton/shards/sqlengine/insert.h"
#include "pelton/shards/sqlengine/gdpr.h"

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
using VV = std::vector<std::vector<std::string>>;
using SN = util::ShardName;

/*
 * The tests!
 */

// Define a fixture that manages a pelton connection.
PELTON_FIXTURE(GDPRGetTest);

TEST_F(GDPRGetTest, Datasubject) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});

  // Make a pelton connection.
  Connection conn = CreateConnection();

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

  // Validate get.
  std::string get = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get, &conn).ResultSets(), (VV{(V{row1})}));
}

TEST_F(GDPRGetTest, DirectTable) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  std::string addr = MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(addr, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, __] = MakeInsert("user", {"5", "'u10'"});
  auto &&[addr1, row1] = MakeInsert("addr", {"0", "0"});
  auto &&[addr2, row2] = MakeInsert("addr", {"1", "0"});
  auto &&[addr3, row3] = MakeInsert("addr", {"2", "5"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr1, &conn), 1);
  EXPECT_UPDATE(Execute(addr2, &conn), 1);
  EXPECT_UPDATE(Execute(addr3, &conn), 1);

  // Validate get.
  std::string get = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get, &conn).ResultSets(), (VV{(V{u0}), (V{row1, row2})}));
}

TEST_F(GDPRGetTest, UnshardedTable) {
  // Parse create table statements.
  std::string unsharded = MakeCreate("unsharded", {"id" I PK, "name" STR});
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  std::string addr = MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(addr, &conn));
  EXPECT_SUCCESS(Execute(unsharded, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
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

  // Validate get.
  std::string get = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get, &conn).ResultSets(), (VV{(V{u0}), (V{row1, row2})}));
}

TEST_F(GDPRGetTest, TransitiveTable) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  std::string addr = MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});
  std::string nums = MakeCreate("phones", {"id" I PK, "aid" I FK "addr(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

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

  // Validate get.
  std::string get = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get, &conn).ResultSets(), (VV{(V{row1, row2}), (V{row5, row6, row7}), (V{row0})}));
}

TEST_F(GDPRGetTest, DeepTransitiveTable) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  std::string addr = MakeCreate("addr", {"id" I PK, "uid" I FK "user(id)"});
  std::string nums = MakeCreate("phones", {"id" I PK, "aid" I FK "addr(id)"});
  std::string deep = MakeCreate("deep", {"id" I PK, "pid" I FK "phones(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(addr, &conn));
  EXPECT_SUCCESS(Execute(nums, &conn));
  EXPECT_SUCCESS(Execute(deep, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u__] = MakeInsert("user", {"5", "'u10'"});
  auto &&[addr1, a0] = MakeInsert("addr", {"0", "0"});
  auto &&[addr2, a1] = MakeInsert("addr", {"1", "0"});
  auto &&[addr3, a___] = MakeInsert("addr", {"2", "5"});
  auto &&[num1, p_] = MakeInsert("phones", {"0", "2"});
  auto &&[num2, p1] = MakeInsert("phones", {"1", "1"});
  auto &&[num3, p2] = MakeInsert("phones", {"2", "0"});
  auto &&[num4, p3] = MakeInsert("phones", {"3", "0"});
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

  // Validate get.
  std::string get = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get, &conn).ResultSets(), 
            (VV{(V{row1, row2, row4, row5}), (V{p1, p2, p3}), (V{a0, a1}), (V{u0})}));
}

TEST_F(GDPRGetTest, TwoOwners) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  std::string msg =
      MakeCreate("msg", {"id" I PK, "OWNER_sender" I FK "user(id)",
                         "OWNER_receiver" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(msg, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u__] = MakeInsert("user", {"5", "'u10'"});
  auto &&[usr3, u2] = MakeInsert("user", {"10", "'u100'"});
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

  // Validate get for user with id 0.
  std::string get1 = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(), 
            (VV{(V{row1, row2, row4}), (V{u0})}));

  // Validate get for user with id 10.
  std::string get2 = MakeGDPRGet("user", "10");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(), 
            (VV{(V{row1, row3}), (V{u2})}));
}

TEST_F(GDPRGetTest, OwnerAccessor) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  std::string msg =
      MakeCreate("msg", {"id" I PK, "OWNER_sender" I FK "user(id)",
                         "ACCESSOR_receiver" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

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

  // Validate get for user with id 0.
  std::string get0 = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get0, &conn).ResultSets(), (VV{(V{row1, row2, row4}), (V{u0})}));

  // Validate get for user with id 10.
  std::string get1 = MakeGDPRGet("user", "10");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(), (VV{(V{row1, row3}), (V{u2})}));
}

TEST_F(GDPRGetTest, ShardedDataSubject) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  std::string invited = MakeCreate(
      "invited", {"id" I PK, "PII_name" STR, "inviting_user" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(invited, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u__] = MakeInsert("user", {"5", "'u10'"});
  auto &&[inv1, row1] = MakeInsert("invited", {"0", "'i1'", "5"});
  auto &&[inv2, row2] = MakeInsert("invited", {"1", "'i10'", "0"});
  auto &&[inv3, row3] = MakeInsert("invited", {"5", "'i100'", "0"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(inv1, &conn), 2);
  EXPECT_UPDATE(Execute(inv2, &conn), 2);
  EXPECT_UPDATE(Execute(inv3, &conn), 2);

  // Validate get for user with id 0.
  std::string get1 = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(), 
            (VV{(V{row2, row3}), (V{u0})}));

  // Validate get for invited with id 1.
  std::string get2 = MakeGDPRGet("invited", "1");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(), (VV{(V{row2})}));
}

TEST_F(GDPRGetTest, VariableOwnership) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  std::string grps = MakeCreate("grps", {"gid" I PK});
  std::string assoc =
      MakeCreate("association", {"id" I PK, "OWNING_group" I FK "grps(gid)",
                                 "OWNER_user" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(grps, &conn));
  EXPECT_SUCCESS(Execute(assoc, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u__] = MakeInsert("user", {"5", "'u10'"});
  auto &&[grps1, row1] = MakeInsert("grps", {"0"});
  auto &&[grps2, row2] = MakeInsert("grps", {"1"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(grps1, &conn), 1);
  EXPECT_UPDATE(Execute(grps2, &conn), 1);

  // Associate groups with some users.
  auto &&[assoc1, a0] = MakeInsert("association", {"0", "0", "0"});
  auto &&[assoc2, a1] = MakeInsert("association", {"1", "1", "0"});
  auto &&[assoc3, a___] = MakeInsert("association", {"1", "1", "5"});
  auto &&[assoc4, a____] = MakeInsert("association", {"2", "1", "5"});

  EXPECT_UPDATE(Execute(assoc1, &conn), 3);
  EXPECT_UPDATE(Execute(assoc2, &conn), 3);
  EXPECT_UPDATE(Execute(assoc3, &conn), 2);
  EXPECT_UPDATE(Execute(assoc4, &conn), 1);

  // Validate get for user with id 0.
  std::string get1 = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(), 
            (VV{(V{row1, row2}), (V{a0, a1}), (V{u0})}));
}

TEST_F(GDPRGetTest, ComplexVariableOwnership) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  std::string admin = MakeCreate("admin", {"aid" I PK, "PII_name" STR});
  std::string grps =
      MakeCreate("grps", {"gid" I PK, "OWNER_admin" I FK "admin(aid)"});
  std::string assoc =
      MakeCreate("association", {"sid" I PK, "OWNING_group" I FK "grps(gid)",
                                 "OWNER_user" I FK "user(id)"});
  std::string files =
      MakeCreate("files", {"fid" I PK, "OWNER_creator" I FK "user(id)"});
  std::string fassoc =
      MakeCreate("fassoc", {"fsid" I PK, "OWNING_file" I FK "files(fid)",
                            "OWNER_group" I FK "grps(gid)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(admin, &conn));
  EXPECT_SUCCESS(Execute(grps, &conn));
  EXPECT_SUCCESS(Execute(assoc, &conn));
  EXPECT_SUCCESS(Execute(files, &conn));
  EXPECT_SUCCESS(Execute(fassoc, &conn));

  // Perform some inserts.
  auto &&[usr1, u_] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u1] = MakeInsert("user", {"5", "'u10'"});
  auto &&[adm1, d0] = MakeInsert("admin", {"0", "'a1'"});
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
  auto &&[assoc1, a0] = MakeInsert("association", {"0", "1", "5"});

  EXPECT_UPDATE(Execute(assoc1, &conn), 6);

  // Validate get for user with id 5.
  std::string get1 = MakeGDPRGet("user", "5");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(), 
            (VV{(V{frow1, frow2}), (V{grow2}), (V{u1}), (V{farow1, farow2}), (V{a0})}));

  // Validate get for admin with id 0.
  std::string get2 = MakeGDPRGet("admin", "0");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(), 
            (VV{(V{frow1, frow2}), (V{grow1, grow2}), (V{d0}), (V{farow1, farow2})}));
}

TEST_F(GDPRGetTest, TransitiveAccessorship) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  std::string msg =
      MakeCreate("msg", {"id" I PK, "OWNER_sender" I FK "user(id)",
                         "ACCESSOR_receiver" I FK "user(id)"});
  std::string meta =
      MakeCreate("meta", {"id" I PK, "OWNER_message" I FK "msg(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(msg, &conn));
  EXPECT_SUCCESS(Execute(meta, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u1] = MakeInsert("user", {"5", "'u10'"});
  auto &&[usr3, u2] = MakeInsert("user", {"10", "'u100'"});
  auto &&[msg1, row1] = MakeInsert("msg", {"1", "0", "10"});
  auto &&[msg2, row2] = MakeInsert("msg", {"2", "0", "0"});
  auto &&[msg3, row3] = MakeInsert("msg", {"3", "5", "10"});
  auto &&[msg4, row4] = MakeInsert("msg", {"4", "5", "0"});
  auto &&[meta1, row5] = MakeInsert("meta", {"1", "1"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 1);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);
  EXPECT_UPDATE(Execute(msg3, &conn), 1);
  EXPECT_UPDATE(Execute(msg4, &conn), 1);
  EXPECT_UPDATE(Execute(meta1, &conn), 1);

  // Validate get.
  std::string get = MakeGDPRGet("user", "10");
  EXPECT_EQ(Execute(get, &conn).ResultSets(), (VV{(V{row1, row3}), (V{u2}), (V{row5})}));
}

TEST_F(GDPRGetTest, VariableAccessorship) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  std::string grps = MakeCreate("grps", {"gid" I PK, "OWNER_creator" I FK "user(id)"});
  std::string assoc =
      MakeCreate("association", {"id" I PK, "ACCESSING_group" I FK "grps(gid)",
                                 "OWNER_user" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(grps, &conn));
  EXPECT_SUCCESS(Execute(assoc, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u1] = MakeInsert("user", {"5", "'u10'"});
  auto &&[usr3, u2] = MakeInsert("user", {"10", "'u100'"});
  auto &&[grps1, row1] = MakeInsert("grps", {"0", "0"});
  auto &&[grps2, row2] = MakeInsert("grps", {"1", "0"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(grps1, &conn), 1);
  EXPECT_UPDATE(Execute(grps2, &conn), 1);

  // Associate groups with some users.
  auto &&[assoc1, a0] = MakeInsert("association", {"1", "1", "5"});
  auto &&[assoc2, a1] = MakeInsert("association", {"2", "1", "5"});
  auto &&[assoc3, a2] = MakeInsert("association", {"2", "1", "10"});

  EXPECT_UPDATE(Execute(assoc1, &conn), 1);
  EXPECT_UPDATE(Execute(assoc2, &conn), 1);
  EXPECT_UPDATE(Execute(assoc3, &conn), 1);

  // Validate get for user with id 5.
  std::string get1 = MakeGDPRGet("user", "5");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(), 
            (VV{(V{row2}), (V{a0, a1}), (V{u1})}));

  // Validate get for user with id 10.
  std::string get2 = MakeGDPRGet("user", "10");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(), 
            (VV{(V{row2}), (V{a2}), (V{u2})}));
}

TEST_F(GDPRGetTest, SimpleMixedAccessorship) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  std::string grps = MakeCreate("grps", {"gid" I PK, "OWNER_creator" I FK "user(id)"});
  std::string meta = MakeCreate("meta", {"id" I PK, "OWNER_group" I FK "grps(gid)"});
  std::string assoc =
      MakeCreate("association", {"id" I PK, "ACCESSING_group" I FK "grps(gid)",
                                 "OWNER_user" I FK "user(id)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(grps, &conn));
  EXPECT_SUCCESS(Execute(assoc, &conn));
  EXPECT_SUCCESS(Execute(meta, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u1] = MakeInsert("user", {"5", "'u10'"});
  auto &&[usr3, u2] = MakeInsert("user", {"10", "'u100'"});
  auto &&[grps1, row1] = MakeInsert("grps", {"0", "0"});
  auto &&[grps2, row2] = MakeInsert("grps", {"1", "0"});
  auto &&[meta1, row3] = MakeInsert("meta", {"0", "0"});
  auto &&[meta2, row4] = MakeInsert("meta", {"1", "1"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(grps1, &conn), 1);
  EXPECT_UPDATE(Execute(grps2, &conn), 1);
  EXPECT_UPDATE(Execute(meta1, &conn), 1);
  EXPECT_UPDATE(Execute(meta2, &conn), 1);

  // Associate groups with some users.
  auto &&[assoc1, a0] = MakeInsert("association", {"1", "1", "5"});
  auto &&[assoc2, a1] = MakeInsert("association", {"2", "1", "5"});
  auto &&[assoc3, a2] = MakeInsert("association", {"2", "1", "10"});

  EXPECT_UPDATE(Execute(assoc1, &conn), 1);
  EXPECT_UPDATE(Execute(assoc2, &conn), 1);
  EXPECT_UPDATE(Execute(assoc3, &conn), 1);

  // Validate get for user with id 5.
  std::string get1 = MakeGDPRGet("user", "5");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(), 
            (VV{(V{row2}), (V{a0, a1}), (V{u1}), (V{row4})}));

  // Validate get for user with id 10.
  std::string get2 = MakeGDPRGet("user", "10");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(), 
            (VV{(V{row2}), (V{a2}), (V{u2}), (V{row4})}));
}

TEST_F(GDPRGetTest, ComplexVariableAccessorship) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
  std::string admin = MakeCreate("admin", {"aid" I PK, "PII_name" STR});
  std::string grps =
      MakeCreate("grps", {"gid" I PK, "OWNER_admin" I FK "admin(aid)"});
  std::string assoc =
      MakeCreate("association", {"sid" I PK, "ACCESSING_group" I FK "grps(gid)",
                                 "OWNER_user" I FK "user(id)"});
  std::string files =
      MakeCreate("files", {"fid" I PK, "OWNER_creator" I FK "user(id)"});
  std::string fassoc =
      MakeCreate("fassoc", {"fsid" I PK, "ACCESSING_file" I FK "files(fid)",
                            "OWNER_group" I FK "grps(gid)"});

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(admin, &conn));
  EXPECT_SUCCESS(Execute(grps, &conn));
  EXPECT_SUCCESS(Execute(assoc, &conn));
  EXPECT_SUCCESS(Execute(files, &conn));
  EXPECT_SUCCESS(Execute(fassoc, &conn));

  // Perform some inserts.
  auto &&[usr1, u_] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u1] = MakeInsert("user", {"5", "'u10'"});
  auto &&[adm1, d0] = MakeInsert("admin", {"0", "'a1'"});
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
  EXPECT_UPDATE(Execute(fa1, &conn), 1);
  EXPECT_UPDATE(Execute(fa2, &conn), 1);

  // Associate everything with a user.
  auto &&[assoc1, a0] = MakeInsert("association", {"0", "1", "5"});

  EXPECT_UPDATE(Execute(assoc1, &conn), 1);

  // Validate get for user with id 5.
  std::string get1 = MakeGDPRGet("user", "5");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(), 
            (VV{(V{frow1, frow2}), (V{grow2}), (V{u1}), (V{farow1, farow2}), (V{a0})}));

  // Validate get for admin with id 0.
  std::string get2 = MakeGDPRGet("admin", "0");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(), 
            (VV{(V{frow1, frow2}), (V{grow1, grow2}), (V{d0}), (V{farow1, farow2})}));
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
