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
K9DB_FIXTURE(GDPRGetAnonTest);

TEST_F(GDPRGetAnonTest, SimpleAnon) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg =
      MakeCreate("story", {"id" I PK, "user" I OB "user(id)", "internal" I},
                 false, "," ON_GET "user" ANON "(internal)");

  // Make a k9db connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(msg, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u1] = MakeInsert("user", {"5", "'u10'"});
  auto &&[str1, s0] = MakeInsert("story", {"0", "0", "100"});
  auto &&[str2, s1] = MakeInsert("story", {"1", "0", "200"});
  auto &&[str3, s2] = MakeInsert("story", {"2", "5", "300"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(str1, &conn), 1);
  EXPECT_UPDATE(Execute(str2, &conn), 1);
  EXPECT_UPDATE(Execute(str3, &conn), 1);

  std::string anon0 = "|0|0|NULL|";
  std::string anon1 = "|1|0|NULL|";
  std::string anon2 = "|2|5|NULL|";

  // Validate anon on get for user with id 0.
  std::string get1 = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(),
            (VV{(V{anon0, anon1}), (V{u0})}));

  // Validate anon on get for user with id 5.
  std::string get2 = MakeGDPRGet("user", "5");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(), (VV{(V{anon2}), (V{u1})}));
}

TEST_F(GDPRGetAnonTest, TwoOwnersAnon) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "user(id)", "receiver" I OB "user(id)"},
      false,
      "," ON_GET "receiver" ANON "(sender)," ON_GET "sender" ANON "(receiver)");

  // Make a k9db connection.
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
  EXPECT_UPDATE(Execute(msg1, &conn), 2);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);  // Inserted only once for u1.
  EXPECT_UPDATE(Execute(msg3, &conn), 2);
  EXPECT_UPDATE(Execute(msg4, &conn), 2);

  // Validate anon on get for user with id 5.
  std::string get1 = MakeGDPRGet("user", "5");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(),
            (VV{(V{"|3|5|NULL|", "|4|5|NULL|"}), (V{u1})}));

  // Validate anon on get for user with id 10.
  std::string get2 = MakeGDPRGet("user", "10");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(),
            (VV{(V{"|1|NULL|10|", "|3|NULL|10|"}), (V{u2})}));

  // Validate anon on get for user with id 0.
  std::string get3 = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get3, &conn).ResultSets(),
            (VV{(V{"|1|0|NULL|", "|2|NULL|NULL|", "|4|NULL|0|"}), (V{u0})}));
}

TEST_F(GDPRGetAnonTest, TwoOwnersAnonDel) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "user(id)", "receiver" I OB "user(id)"},
      false, "," ON_GET "receiver" ANON "(sender)," ON_GET "sender" DEL_ROW);

  // Make a k9db connection.
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
  EXPECT_UPDATE(Execute(msg1, &conn), 2);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);  // Inserted only once for u1.
  EXPECT_UPDATE(Execute(msg3, &conn), 2);
  EXPECT_UPDATE(Execute(msg4, &conn), 2);

  // Validate anon on get for user with id 5.
  std::string get1 = MakeGDPRGet("user", "5");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(), (VV{(V{}), (V{u1})}));

  // Validate anon on get for user with id 10.
  std::string get2 = MakeGDPRGet("user", "10");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(),
            (VV{(V{"|1|NULL|10|", "|3|NULL|10|"}), (V{u2})}));

  // Validate anon on get for user with id 0.
  std::string get3 = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get3, &conn).ResultSets(),
            (VV{(V{"|4|NULL|0|"}), (V{u0})}));
}

TEST_F(GDPRGetAnonTest, OwnerAccessorAnon) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "user(id)", "receiver" I AB "user(id)"},
      false,
      "," ON_GET "receiver" ANON "(sender)," ON_GET "sender" ANON "(receiver)");

  // Make a k9db connection.
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

  // Validate anon on get for user with id 5.
  std::string get1 = MakeGDPRGet("user", "5");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(),
            (VV{(V{"|3|5|NULL|", "|4|5|NULL|"}), (V{u1})}));

  // Validate anon on get for user with id 10.
  std::string get2 = MakeGDPRGet("user", "10");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(),
            (VV{(V{"|1|NULL|10|", "|3|NULL|10|"}), (V{u2})}));

  // Validate anon on get for user with id 0.
  std::string get3 = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get3, &conn).ResultSets(),
            (VV{(V{"|1|0|NULL|", "|2|NULL|NULL|", "|4|NULL|0|"}), (V{u0})}));
}

TEST_F(GDPRGetAnonTest, MultipleColumnsAnonAdmin) {
  // Parse create table statements.
  std::string admn = MakeCreate("admin", {"aid" I PK, "aname" STR}, true);
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg",
      {"id" I PK, "creator" I OB "admin(aid)", "sender" I AB "user(id)",
       "receiver" I AB "user(id)", "secret1" STR, "secret2" STR},
      false,
      "," ON_GET "receiver" ANON "(sender, secret1, secret2, creator)," ON_GET
      "sender" ANON "(receiver, secret1, creator)");

  // Make a k9db connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(admn, &conn));
  EXPECT_SUCCESS(Execute(msg, &conn));

  // // Perform some inserts.
  auto &&[admn1, a0] = MakeInsert("admin", {"0", "'a1'"});
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u1] = MakeInsert("user", {"5", "'u10'"});
  auto &&[usr3, u2] = MakeInsert("user", {"10", "'u100'"});
  auto &&[msg1, row1] =
      MakeInsert("msg", {"1", "0", "0", "10", "'secret1'", "'secret2'"});
  auto &&[msg2, row2] =
      MakeInsert("msg", {"2", "0", "0", "0", "'secret1'", "'secret2'"});
  auto &&[msg3, row3] =
      MakeInsert("msg", {"3", "0", "5", "10", "'secret1'", "'secret2'"});
  auto &&[msg4, row4] =
      MakeInsert("msg", {"4", "0", "5", "0", "'secret1'", "'secret2'"});

  EXPECT_UPDATE(Execute(admn1, &conn), 1);
  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 1);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);
  EXPECT_UPDATE(Execute(msg3, &conn), 1);
  EXPECT_UPDATE(Execute(msg4, &conn), 1);

  // Validate anon on get for user with id 5.
  std::string get1 = MakeGDPRGet("user", "5");
  EXPECT_EQ(
      Execute(get1, &conn).ResultSets(),
      (VV{(V{"|3|NULL|5|NULL|NULL|secret2|", "|4|NULL|5|NULL|NULL|secret2|"}),
          (V{u1})}));

  // // Validate anon on get for user with id 10.
  std::string get2 = MakeGDPRGet("user", "10");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(),
            (VV{(V{"|1|NULL|NULL|10|NULL|NULL|", "|3|NULL|NULL|10|NULL|NULL|"}),
                (V{u2})}));

  // Validate anon on get for user with id 0.
  // Anonymization rules between both access paths should merge (set
  // intersection).
  std::string get3 = MakeGDPRGet("user", "0");
  EXPECT_EQ(
      Execute(get3, &conn).ResultSets(),
      (VV{(V{"|1|NULL|0|NULL|NULL|secret2|", "|2|NULL|NULL|NULL|NULL|NULL|",
             "|4|NULL|NULL|0|NULL|NULL|"}),
          (V{u0})}));
}

TEST_F(GDPRGetAnonTest, MultipleColumnsAnon) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg",
      {"id" I PK, "sender" I OB "user(id)", "receiver" I AB "user(id)",
       "sender_secret" STR, "receiver_secret" STR},
      false,
      "," ON_GET "receiver" ANON "(sender, sender_secret)," ON_GET "sender" ANON
      "(receiver, receiver_secret)");

  // Make a k9db connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(msg, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u1] = MakeInsert("user", {"5", "'u10'"});
  auto &&[usr3, u2] = MakeInsert("user", {"10", "'u100'"});
  auto &&[msg1, row1] =
      MakeInsert("msg", {"1", "0", "10", "'secret1'", "'secret2'"});
  auto &&[msg2, row2] =
      MakeInsert("msg", {"2", "0", "0", "'secret1'", "'secret2'"});
  auto &&[msg3, row3] =
      MakeInsert("msg", {"3", "5", "10", "'secret1'", "'secret2'"});
  auto &&[msg4, row4] =
      MakeInsert("msg", {"4", "5", "0", "'secret1'", "'secret2'"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 1);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);
  EXPECT_UPDATE(Execute(msg3, &conn), 1);
  EXPECT_UPDATE(Execute(msg4, &conn), 1);

  // Validate anon on get for user with id 5.
  std::string get1 = MakeGDPRGet("user", "5");
  EXPECT_EQ(
      Execute(get1, &conn).ResultSets(),
      (VV{(V{"|3|5|NULL|secret1|NULL|", "|4|5|NULL|secret1|NULL|"}), (V{u1})}));

  // // Validate anon on get for user with id 10.
  std::string get2 = MakeGDPRGet("user", "10");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(),
            (VV{(V{"|1|NULL|10|NULL|secret2|", "|3|NULL|10|NULL|secret2|"}),
                (V{u2})}));

  // Validate anon on get for user with id 0.
  // Anonymization rules between both access paths should merge (set
  // intersection).
  std::string get3 = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get3, &conn).ResultSets(),
            (VV{(V{"|1|0|NULL|secret1|NULL|", "|2|NULL|NULL|NULL|NULL|",
                   "|4|NULL|0|NULL|secret2|"}),
                (V{u0})}));
}

TEST_F(GDPRGetAnonTest, TwoDistinctOwnerAccessorAnon) {
  // Parse create table statements.
  std::string patient = MakeCreate("patient", {"id" I PK, "name" STR}, true);
  std::string doctor = MakeCreate("doctor", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg",
      {"id" I PK, "sender" I OB "doctor(id)", "receiver" I AB "patient(id)"},
      false,
      "," ON_GET "receiver" ANON "(sender)," ON_GET "sender" ANON "(receiver)");

  // Make a k9db connection.
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

TEST_F(GDPRGetAnonTest, TwoDistinctOwnersAnon) {
  // Parse create table statements.
  std::string patient = MakeCreate("patient", {"id" I PK, "name" STR}, true);
  std::string doctor = MakeCreate("doctor", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg",
      {"id" I PK, "sender" I OB "doctor(id)", "receiver" I OB "patient(id)"},
      false,
      "," ON_GET "receiver" ANON "(sender)," ON_GET "sender" ANON "(receiver)");

  // Make a k9db connection.
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
  EXPECT_UPDATE(Execute(msg1, &conn), 2);
  EXPECT_UPDATE(Execute(msg2, &conn), 2);
  EXPECT_UPDATE(Execute(msg3, &conn), 2);

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

TEST_F(GDPRGetAnonTest, TransitiveOwnershipAnon) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string prof = MakeCreate("prof", {"id" I PK, "uid" I FK "user(id)"});
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "prof(id)", "receiver" I OB "prof(id)"},
      false,
      "," ON_GET "receiver" ANON "(sender)," ON_GET "sender" ANON "(receiver)");

  // Make a k9db connection.
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
  auto &&[_, row4_anon1] = MakeInsert("msg", {"1", "NULL", "2"});
  auto &&[__, row4_anon2] = MakeInsert("msg", {"1", "0", "NULL"});
  auto &&[msg2, row5] = MakeInsert("msg", {"2", "0", "0"});
  auto &&[___, row5_anon] = MakeInsert("msg", {"2", "NULL", "NULL"});
  auto &&[msg3, row6] = MakeInsert("msg", {"3", "1", "2"});
  auto &&[____, row6_anon1] = MakeInsert("msg", {"3", "NULL", "2"});
  auto &&[_____, row6_anon2] = MakeInsert("msg", {"3", "1", "NULL"});
  auto &&[msg4, row7] = MakeInsert("msg", {"4", "2", "0"});
  auto &&[______, row7_anon1] = MakeInsert("msg", {"4", "2", "NULL"});
  auto &&[_______, row7_anon2] = MakeInsert("msg", {"4", "NULL", "0"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(prof1, &conn), 1);
  EXPECT_UPDATE(Execute(prof2, &conn), 1);
  EXPECT_UPDATE(Execute(prof3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 2);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);
  EXPECT_UPDATE(Execute(msg3, &conn), 2);
  EXPECT_UPDATE(Execute(msg4, &conn), 2);

  // Validate anon on get for user with id 0.
  std::string get1 = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(),
            (VV{(V{row1, row2}),
                (V{row4_anon2, row5_anon, row6_anon2, row7_anon2}), (V{u0})}));

  // Validate anon on get for user with id 5.
  std::string get2 = MakeGDPRGet("user", "5");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(),
            (VV{(V{row3}), (V{row4_anon1, row6_anon1, row7_anon1}), (V{u1})}));
}

TEST_F(GDPRGetAnonTest, ComplexVariableOwnershipAnon) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string admin = MakeCreate("admin", {"aid" I PK, "name" STR}, true);
  std::string grps =
      MakeCreate("grps", {"gid" I PK, "admin" I OB "admin(aid)"});
  std::string assoc = MakeCreate(
      "association",
      {"sid" I PK, "group_id" I OW "grps(gid)", "user_id" I OB "user(id)"});
  std::string files =
      MakeCreate("files", {"fid" I PK, "creator" I OB "user(id)"}, false,
                 "," ON_GET "fassoc(file)" ANON "(creator)," ON_GET
                 "creator" ANON "(fid)");
  std::string fassoc = MakeCreate(
      "fassoc",
      {"fsid" I PK, "file" I OW "files(fid)", "group_id" I OB "grps(gid)"});

  // Make a k9db connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(admin, &conn));
  EXPECT_SUCCESS(Execute(grps, &conn));
  EXPECT_SUCCESS(Execute(assoc, &conn));
  EXPECT_SUCCESS(Execute(files, &conn));
  EXPECT_SUCCESS(Execute(fassoc, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
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
  std::string fullanon = "|NULL|NULL|";
  std::string fanon1 = "|0|NULL|";
  std::string fanon2 = "|1|NULL|";

  EXPECT_UPDATE(Execute(f1, &conn), 1);
  EXPECT_UPDATE(Execute(f2, &conn), 1);
  EXPECT_UPDATE(Execute(fa1, &conn), 2);
  EXPECT_UPDATE(Execute(fa2, &conn), 2);

  // Associate everything with a user.
  auto &&[assoc1, a0] = MakeInsert("association", {"0", "1", "5"});
  auto &&[assoc2, a1] = MakeInsert("association", {"1", "1", "0"});

  EXPECT_UPDATE(Execute(assoc1, &conn), 6);
  EXPECT_UPDATE(Execute(assoc2, &conn), 4);

  // Validate anon on get for user with id 0.
  std::string get1 = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(),
            (VV{(V{u0}), (V{grow2}), (V{a1}), (V{farow1, farow2}),
                (V{fullanon, fullanon})}));

  // Validate anon on get for user with id 5.
  std::string get2 = MakeGDPRGet("user", "5");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(),
            (VV{(V{u1}), (V{grow2}), (V{a0}), (V{farow1, farow2}),
                (V{fanon1, fanon2})}));

  // Validate anon on get for admin with id 0.
  std::string get3 = MakeGDPRGet("admin", "0");
  EXPECT_EQ(Execute(get3, &conn).ResultSets(),
            (VV{(V{d0}), (V{grow1, grow2}), (V{farow1, farow2}),
                (V{fanon1, fanon2})}));
}

TEST_F(GDPRGetAnonTest, ComplexVariableAccessorship) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string admin = MakeCreate("admin", {"aid" I PK, "name" STR}, true);
  std::string grps =
      MakeCreate("grps", {"gid" I PK, "admin" I OB "admin(aid)"});
  std::string assoc = MakeCreate(
      "association",
      {"sid" I PK, "group_id" I AC "grps(gid)", "user_id" I OB "user(id)"});
  std::string files =
      MakeCreate("files", {"fid" I PK, "creator" I OB "user(id)"}, false,
                 "," ON_GET "fassoc(file)" ANON "(creator)," ON_GET
                 "creator" ANON "(fid)");
  std::string fassoc = MakeCreate(
      "fassoc",
      {"fsid" I PK, "file" I OW "files(fid)", "group_id" I OB "grps(gid)"});

  // Make a k9db connection.
  Connection conn = CreateConnection();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(admin, &conn));
  EXPECT_SUCCESS(Execute(grps, &conn));
  EXPECT_SUCCESS(Execute(assoc, &conn));
  EXPECT_SUCCESS(Execute(files, &conn));
  EXPECT_SUCCESS(Execute(fassoc, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
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
  std::string fullanon = "|NULL|NULL|";
  std::string fanon1 = "|0|NULL|";
  std::string fanon2 = "|1|NULL|";

  EXPECT_UPDATE(Execute(f1, &conn), 1);
  EXPECT_UPDATE(Execute(f2, &conn), 1);
  EXPECT_UPDATE(Execute(fa1, &conn), 2);
  EXPECT_UPDATE(Execute(fa2, &conn), 2);

  // Associate everything with a user.
  auto &&[assoc1, a0] = MakeInsert("association", {"0", "1", "5"});
  auto &&[assoc2, a1] = MakeInsert("association", {"1", "1", "0"});

  EXPECT_UPDATE(Execute(assoc1, &conn), 1);
  EXPECT_UPDATE(Execute(assoc2, &conn), 1);

  // Validate anon on get for user with id 0.
  std::string get1 = MakeGDPRGet("user", "0");
  EXPECT_EQ(Execute(get1, &conn).ResultSets(),
            (VV{(V{u0}), (V{grow2}), (V{a1}), (V{farow1, farow2}),
                (V{fullanon, fullanon})}));

  // Validate anon on get for user with id 5.
  std::string get2 = MakeGDPRGet("user", "5");
  EXPECT_EQ(Execute(get2, &conn).ResultSets(),
            (VV{(V{u1}), (V{grow2}), (V{a0}), (V{farow1, farow2}),
                (V{fanon1, fanon2})}));

  // Validate anon on get for admin with id 0.
  std::string get3 = MakeGDPRGet("admin", "0");
  EXPECT_EQ(Execute(get3, &conn).ResultSets(),
            (VV{(V{d0}), (V{grow1, grow2}), (V{farow1, farow2}),
                (V{fanon1, fanon2})}));
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
