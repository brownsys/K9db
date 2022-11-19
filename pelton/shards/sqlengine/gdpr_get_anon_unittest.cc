#include <string>
#include <unordered_set>
#include <vector>

#include "glog/logging.h"
#include "pelton/shards/sqlengine/gdpr.h"
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
PELTON_FIXTURE(GDPRGetAnonTest);

TEST_F(GDPRGetAnonTest, TwoOwnersAnon) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "user(id)", "receiver" I OB "user(id)"},
      false,
      "," ON_GET "receiver" ANON "(sender)," ON_GET "sender" ANON "(receiver)");

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
  auto &&[_, row1_anon] = MakeInsert("msg", {"1", "NULL", "10"});
  auto &&[msg2, row2] = MakeInsert("msg", {"2", "0", "0"});
  auto &&[msg3, row3] = MakeInsert("msg", {"3", "5", "10"});
  auto &&[__, row3_anon1] = MakeInsert("msg", {"3", "NULL", "10"});
  auto &&[___, row3_anon2] = MakeInsert("msg", {"3", "5", "NULL"});
  auto &&[msg4, row4] = MakeInsert("msg", {"4", "5", "0"});
  auto &&[____, row4_anon] = MakeInsert("msg", {"4", "5", "NULL"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 2);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);  // Inserted only once for u1.
  EXPECT_UPDATE(Execute(msg3, &conn), 2);
  EXPECT_UPDATE(Execute(msg4, &conn), 2);

  // Validate anon on get for user with id 5.
  std::string get1 = MakeGDPRGet("user", "5");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(),
            (VV{(V{row3_anon2, row4_anon}), (V{u1})}));

  // Validate anon on get for user with id 10.
  std::string get2 = MakeGDPRGet("user", "10");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(),
            (VV{(V{row1_anon, row3_anon1}), (V{u2})}));
}

TEST_F(GDPRGetAnonTest, OwnerAccessorAnon) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "user(id)", "receiver" I AB "user(id)"},
      false,
      "," ON_GET "receiver" ANON "(sender)," ON_GET "sender" ANON "(receiver)");

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
  auto &&[_, row1_anon] = MakeInsert("msg", {"1", "NULL", "10"});
  auto &&[msg2, row2] = MakeInsert("msg", {"2", "0", "0"});
  auto &&[msg3, row3] = MakeInsert("msg", {"3", "5", "10"});
  auto &&[__, row3_anon1] = MakeInsert("msg", {"3", "NULL", "10"});
  auto &&[___, row3_anon2] = MakeInsert("msg", {"3", "5", "NULL"});
  auto &&[msg4, row4] = MakeInsert("msg", {"4", "5", "0"});
  auto &&[____, row4_anon] = MakeInsert("msg", {"4", "5", "NULL"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 1);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);
  EXPECT_UPDATE(Execute(msg3, &conn), 1);
  EXPECT_UPDATE(Execute(msg4, &conn), 1);

  // Validate anon on get for user with id 5.
  std::string get1 = MakeGDPRGet("user", "5");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(),
            (VV{(V{row3_anon2, row4_anon}), (V{u1})}));

  // Validate anon on get for user with id 10.
  std::string get2 = MakeGDPRGet("user", "10");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(),
            (VV{(V{row1_anon, row3_anon1}), (V{u2})}));
}

TEST_F(GDPRGetAnonTest, TwoDistinctOwnersAnon) {
  // Parse create table statements.
  std::string patient = MakeCreate("patient", {"id" I PK, "name" STR}, true);
  std::string doctor = MakeCreate("doctor", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg",
      {"id" I PK, "sender" I OB "doctor(id)", "receiver" I AB "patient(id)"},
      false,
      "," ON_GET "receiver" ANON "(sender)," ON_GET "sender" ANON "(receiver)");

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(patient, &conn));
  EXPECT_SUCCESS(Execute(doctor, &conn));
  EXPECT_SUCCESS(Execute(msg, &conn));

  // Perform some inserts.
  auto &&[pat1, p0] = MakeInsert("patient", {"0", "'p1'"});
  auto &&[pat2, p1] = MakeInsert("patient", {"5", "'p10'"});
  auto &&[doc1, d0] = MakeInsert("doctor", {"0", "'d1'"});
  auto &&[doc2, d1] = MakeInsert("doctor", {"5", "'d10'"});

  auto &&[msg1, row1] = MakeInsert("msg", {"1", "0", "0"});
  auto &&[_, row1_anon1] = MakeInsert("msg", {"1", "NULL", "0"});
  auto &&[__, row1_anon2] = MakeInsert("msg", {"1", "0", "NULL"});

  auto &&[msg2, row2] = MakeInsert("msg", {"2", "0", "5"});
  auto &&[___, row2_anon1] = MakeInsert("msg", {"2", "NULL", "5"});
  auto &&[____, row2_anon2] = MakeInsert("msg", {"2", "0", "NULL"});

  auto &&[msg3, row3] = MakeInsert("msg", {"3", "5", "0"});
  auto &&[_____, row3_anon1] = MakeInsert("msg", {"3", "NULL", "0"});
  auto &&[______, row3_anon2] = MakeInsert("msg", {"3", "5", "NULL"});

  EXPECT_UPDATE(Execute(pat1, &conn), 1);
  EXPECT_UPDATE(Execute(pat2, &conn), 1);
  EXPECT_UPDATE(Execute(doc1, &conn), 1);
  EXPECT_UPDATE(Execute(doc2, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 1);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);
  EXPECT_UPDATE(Execute(msg3, &conn), 1);

  // Validate anon on get for patient with id 0.
  std::string get1 = MakeGDPRGet("patient", "0");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(),
            (VV{(V{row1_anon1, row3_anon1}), (V{p0})}));

  // Validate anon on get for patient with id 5.
  std::string get2 = MakeGDPRGet("patient", "5");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(), (VV{(V{row2_anon1}), (V{p1})}));

  // Validate anon on get for doctor with id 0.
  std::string get3 = MakeGDPRGet("doctor", "0");
  EXPECT_EQ(Execute(get3, &conn).ResultSets(),
            (VV{(V{row1_anon2, row2_anon2}), (V{d0})}));

  // Validate anon on get for doctor with id 5.
  std::string get4 = MakeGDPRGet("doctor", "5");
  EXPECT_EQ(Execute(get4, &conn).ResultSets(), (VV{(V{row3_anon2}), (V{d1})}));
}

TEST_F(GDPRGetAnonTest, TransitiveAnon) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string prof = MakeCreate("prof", {"id" I PK, "uid" I FK "user(id)"});
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "prof(id)", "receiver" I OB "prof(id)"},
      false,
      "," ON_GET "receiver" ANON "(sender)," ON_GET "sender" ANON "(receiver)");

  // Make a pelton connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(prof, &conn));
  EXPECT_SUCCESS(Execute(msg, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u1] = MakeInsert("user", {"5", "'u10'"});
  auto &&[prof1, row1] = MakeInsert("prof", {"0", "0"});
  auto &&[prof2, row2] = MakeInsert("prof", {"1", "0"});
  auto &&[prof3, row3] = MakeInsert("prof", {"2", "5"});
  auto &&[msg1, row4] = MakeInsert("msg", {"1", "0", "2"});
  auto &&[_, row4_anon] = MakeInsert("msg", {"1", "NULL", "2"});
  auto &&[msg2, row5] = MakeInsert("msg", {"2", "0", "0"});
  auto &&[msg3, row6] = MakeInsert("msg", {"3", "1", "2"});
  auto &&[__, row6_anon1] = MakeInsert("msg", {"3", "NULL", "2"});
  auto &&[___, row6_anon2] = MakeInsert("msg", {"3", "1", "NULL"});
  auto &&[msg4, row7] = MakeInsert("msg", {"4", "2", "0"});
  auto &&[____, row7_anon] = MakeInsert("msg", {"4", "2", "NULL"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(prof1, &conn), 1);
  EXPECT_UPDATE(Execute(prof2, &conn), 1);
  EXPECT_UPDATE(Execute(prof3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 2);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);
  EXPECT_UPDATE(Execute(msg3, &conn), 2);
  EXPECT_UPDATE(Execute(msg4, &conn), 2);

  // Validate anon on get.
  std::string get = MakeGDPRGet("user", "5");
  EXPECT_EQ(Execute(get, &conn).ResultSets(),
            (VV{(V{row3}), (V{row4_anon, row6_anon1, row7_anon}), (V{u1})}));
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
