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
PELTON_FIXTURE(GDPRForgetAnonTest);

TEST_F(GDPRForgetAnonTest, TwoOwnersAnon) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "user(id)", "receiver" I OB "user(id)"},
      false,
      "," ON_DEL "receiver" ANON "(receiver)," ON_DEL "sender" ANON "(sender)");

  // Make a pelton connection.
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
  auto &&[_, row1_anon] = MakeInsert("msg", {"1", "600", "10"});
  auto &&[msg2, row2] = MakeInsert("msg", {"2", "0", "0"});
  auto &&[msg3, row3] = MakeInsert("msg", {"3", "5", "10"});
  auto &&[msg4, row4] = MakeInsert("msg", {"4", "5", "0"});
  auto &&[__, row4_anon] = MakeInsert("msg", {"4", "5", "600"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 2);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);  // Inserted only once for u1.
  EXPECT_UPDATE(Execute(msg3, &conn), 2);
  EXPECT_UPDATE(Execute(msg4, &conn), 2);

  // Validate anon on forget for user with id 0.
  std::string forget = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget, &conn), 10);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("msg", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "5")), (V{row3, row4_anon}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "10")), (V{row1_anon, row3}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetAnonTest, OwnerAccessorAnon) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "user(id)", "receiver" I AB "user(id)"},
      false, "," ON_DEL "receiver" ANON "(receiver)");

  // Make a pelton connection.
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
  auto &&[_, row4_anon] = MakeInsert("msg", {"4", "5", "600"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 1);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);
  EXPECT_UPDATE(Execute(msg3, &conn), 1);
  EXPECT_UPDATE(Execute(msg4, &conn), 1);

  // Validate anon on forget for user with id 0.
  std::string forget = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget, &conn), 3);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("msg", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "5")), (V{row3, row4_anon}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "10")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetAnonTest, MultipleColumnsAnon) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg",
      {"id" I PK, "sender" I OB "user(id)", "receiver" I AB "user(id)",
       "sender_secret" I, "receiver_secret" I},
      false, "," ON_DEL "receiver" ANON "(receiver, receiver_secret)");

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

  // Create the tables.
  EXPECT_SUCCESS(Execute(usr, &conn));
  EXPECT_SUCCESS(Execute(msg, &conn));

  // Perform some inserts.
  auto &&[usr1, u0] = MakeInsert("user", {"0", "'u1'"});
  auto &&[usr2, u1] = MakeInsert("user", {"5", "'u10'"});
  auto &&[usr3, u2] = MakeInsert("user", {"10", "'u100'"});
  auto &&[msg1, row1] = MakeInsert("msg", {"1", "0", "10", "123", "234"});
  auto &&[msg2, row2] = MakeInsert("msg", {"2", "0", "0", "123", "234"});
  auto &&[msg3, row3] = MakeInsert("msg", {"3", "5", "10", "123", "234"});
  auto &&[msg4, row4] = MakeInsert("msg", {"4", "5", "0", "123", "234"});
  auto &&[_, row4_anon] = MakeInsert("msg", {"4", "5", "600", "123", "600"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(usr3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 1);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);
  EXPECT_UPDATE(Execute(msg3, &conn), 1);
  EXPECT_UPDATE(Execute(msg4, &conn), 1);

  // Validate anon on forget for user with id 0.
  std::string forget = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget, &conn), 3);

  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("msg", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "5")), (V{row3, row4_anon}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "10")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetAnonTest, TwoDistinctOwnersAnon) {
  // Parse create table statements.
  std::string patient = MakeCreate("patient", {"id" I PK, "name" STR}, true);
  std::string doctor = MakeCreate("doctor", {"id" I PK, "name" STR}, true);
  std::string msg = MakeCreate(
      "msg",
      {"id" I PK, "sender" I OB "doctor(id)", "receiver" I OB "patient(id)"},
      false,
      "," ON_DEL "receiver" ANON "(receiver)," ON_DEL "sender" ANON "(sender)");

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

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
  auto &&[_, row1_anon] = MakeInsert("msg", {"1", "0", "600"});

  auto &&[msg2, row2] = MakeInsert("msg", {"2", "0", "5"});

  auto &&[msg3, row3] = MakeInsert("msg", {"3", "5", "0"});
  auto &&[__, row3_anon] = MakeInsert("msg", {"3", "5", "600"});

  EXPECT_UPDATE(Execute(pat1, &conn), 1);
  EXPECT_UPDATE(Execute(pat2, &conn), 1);
  EXPECT_UPDATE(Execute(doc1, &conn), 1);
  EXPECT_UPDATE(Execute(doc2, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 2);
  EXPECT_UPDATE(Execute(msg2, &conn), 2);
  EXPECT_UPDATE(Execute(msg3, &conn), 2);

  // Validate anon on forget for patient with id 0.
  std::string forget1 = MakeGDPRForget("patient", "0");
  EXPECT_UPDATE(Execute(forget1, &conn), 9);
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("msg", SN("patient", "0")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN("patient", "5")), (V{row2}));
  EXPECT_EQ(db->GetShard("msg", SN("doctor", "0")), (V{row2, row1_anon}));
  EXPECT_EQ(db->GetShard("msg", SN("doctor", "5")), (V{row3_anon}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();

  // Validate anon on forget for doctor with id 5.
  std::string forget2 = MakeGDPRForget("doctor", "5");
  EXPECT_UPDATE(Execute(forget2, &conn), 5);
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("msg", SN("patient", "0")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN("patient", "5")), (V{row2}));
  EXPECT_EQ(db->GetShard("msg", SN("doctor", "0")), (V{row2, row1_anon}));
  EXPECT_EQ(db->GetShard("msg", SN("doctor", "5")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetAnonTest, TransitiveOwnershipAnon) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string prof = MakeCreate("prof", {"id" I PK, "uid" I FK "user(id)"});
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "prof(id)", "receiver" I OB "prof(id)"},
      false,
      "," ON_DEL "receiver" ANON "(receiver)," ON_DEL "sender" ANON "(sender)");

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

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
  auto &&[_, row4_anon] = MakeInsert("msg", {"1", "600", "2"});
  // Sending a message to myself from the same profile
  auto &&[msg2, row5] = MakeInsert("msg", {"2", "0", "0"});
  auto &&[msg3, row6] = MakeInsert("msg", {"3", "1", "2"});
  auto &&[__, row6_anon] = MakeInsert("msg", {"3", "600", "2"});
  auto &&[msg4, row7] = MakeInsert("msg", {"4", "2", "0"});
  auto &&[___, row7_anon] = MakeInsert("msg", {"4", "2", "600"});
  // Sending a message to myself from different profiles
  auto &&[msg5, row8] = MakeInsert("msg", {"5", "0", "1"});
  auto &&[msg6, row9] = MakeInsert("msg", {"6", "1", "0"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(prof1, &conn), 1);
  EXPECT_UPDATE(Execute(prof2, &conn), 1);
  EXPECT_UPDATE(Execute(prof3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 2);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);
  EXPECT_UPDATE(Execute(msg3, &conn), 2);
  EXPECT_UPDATE(Execute(msg4, &conn), 2);
  EXPECT_UPDATE(Execute(msg5, &conn), 1);
  EXPECT_UPDATE(Execute(msg6, &conn), 1);

  // Validate anon on forget for user with id 0.
  std::string forget = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget, &conn), 15);
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("msg", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "5")),
            (V{row4_anon, row6_anon, row7_anon}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetAnonTest, TransitiveAccessorshipAnon) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string prof = MakeCreate("prof", {"id" I PK, "uid" I FK "user(id)"});
  std::string msg = MakeCreate(
      "msg", {"id" I PK, "sender" I OB "prof(id)", "receiver" I AB "prof(id)"},
      false, "," ON_DEL "receiver" ANON "(receiver)");

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::Session *db = conn.session.get();

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
  // Sending a message to myself from the same profile
  auto &&[msg2, row5] = MakeInsert("msg", {"2", "0", "0"});
  // More tests
  auto &&[msg3, row6] = MakeInsert("msg", {"3", "1", "2"});
  auto &&[msg4, row7] = MakeInsert("msg", {"4", "2", "0"});
  auto &&[___, row7_anon] = MakeInsert("msg", {"4", "2", "600"});
  // Sending a message to myself from different profiles
  auto &&[msg5, row8] = MakeInsert("msg", {"5", "0", "1"});
  auto &&[msg6, row9] = MakeInsert("msg", {"6", "1", "0"});

  EXPECT_UPDATE(Execute(usr1, &conn), 1);
  EXPECT_UPDATE(Execute(usr2, &conn), 1);
  EXPECT_UPDATE(Execute(prof1, &conn), 1);
  EXPECT_UPDATE(Execute(prof2, &conn), 1);
  EXPECT_UPDATE(Execute(prof3, &conn), 1);
  EXPECT_UPDATE(Execute(msg1, &conn), 1);
  EXPECT_UPDATE(Execute(msg2, &conn), 1);
  EXPECT_UPDATE(Execute(msg3, &conn), 1);
  EXPECT_UPDATE(Execute(msg4, &conn), 1);
  EXPECT_UPDATE(Execute(msg5, &conn), 1);
  EXPECT_UPDATE(Execute(msg6, &conn), 1);

  // Validate anon on forget for user with id 0.
  std::string forget = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget, &conn), 8);
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("msg", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("msg", SN("user", "5")), (V{row7_anon}));
  EXPECT_EQ(db->GetShard("msg", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetAnonTest, ComplexVariableOwnershipAnon) {
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
                 "," ON_DEL "creator" ANON "(creator)");
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
  auto &&[_, frow1_anon] = MakeInsert("files", {"0", "600"});
  auto &&[f2, frow2] = MakeInsert("files", {"1", "0"});
  auto &&[__, frow2_anon] = MakeInsert("files", {"1", "600"});
  auto &&[fa1, farow1] = MakeInsert("fassoc", {"0", "0", "1"});
  auto &&[fa2, farow2] = MakeInsert("fassoc", {"1", "1", "1"});

  EXPECT_UPDATE(Execute(f1, &conn), 1);
  EXPECT_UPDATE(Execute(f2, &conn), 1);
  EXPECT_UPDATE(Execute(fa1, &conn), 2);
  EXPECT_UPDATE(Execute(fa2, &conn), 2);

  // Associate everything with a user.
  auto &&[assoc1, a0] = MakeInsert("association", {"0", "1", "5"});
  auto &&[assoc2, a1] = MakeInsert("association", {"1", "1", "0"});

  EXPECT_UPDATE(Execute(assoc1, &conn), 6);
  EXPECT_UPDATE(Execute(assoc2, &conn), 4);

  // Validate anon on forget for user with id 0.
  std::string forget = MakeGDPRForget("user", "0");
  EXPECT_UPDATE(Execute(forget, &conn), 19);
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("grps", SN("admin", "0")), (V{grow1, grow2}));
  EXPECT_EQ(db->GetShard("files", SN("admin", "0")),
            (V{frow1_anon, frow2_anon}));
  EXPECT_EQ(db->GetShard("fassoc", SN("admin", "0")), (V{farow1, farow2}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{grow2}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetShard("files", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("files", SN("user", "5")),
            (V{frow1_anon, frow2_anon}));
  EXPECT_EQ(db->GetShard("files", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN("user", "5")), (V{farow1, farow2}));
  db->RollbackTransaction();
}

TEST_F(GDPRForgetAnonTest, ComplexVariableAccessorshipAnon) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "name" STR}, true);
  std::string admin = MakeCreate("admin", {"aid" I PK, "name" STR}, true);
  std::string grps =
      MakeCreate("grps", {"gid" I PK, "admin" I OB "admin(aid)"});
  std::string assoc = MakeCreate(
      "association",
      {"sid" I PK, "group_id" I OW "grps(gid)", "user_id" I OB "user(id)"});
  std::string files = MakeCreate(
      "files", {"fid" I PK, "creator" I OB "user(id)", "group_secret" I}, false,
      "," ON_DEL "fid" ANON "(group_secret)");
  std::string fassoc = MakeCreate(
      "fassoc",
      {"fsid" I PK, "file" I AC "files(fid)", "group_id" I AB "grps(gid)"});

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
  auto &&[f1, frow1] = MakeInsert("files", {"0", "0", "0"});
  auto &&[_, frow1_anon] = MakeInsert("files", {"0", "0", "600"});
  auto &&[f2, frow2] = MakeInsert("files", {"1", "0", "0"});
  auto &&[__, frow2_anon] = MakeInsert("files", {"1", "0", "600"});
  auto &&[fa1, farow1] = MakeInsert("fassoc", {"0", "0", "1"});
  auto &&[fa2, farow2] = MakeInsert("fassoc", {"1", "1", "1"});

  EXPECT_UPDATE(Execute(f1, &conn), 1);
  EXPECT_UPDATE(Execute(f2, &conn), 1);
  EXPECT_UPDATE(Execute(fa1, &conn), 1);
  EXPECT_UPDATE(Execute(fa2, &conn), 1);

  // Associate everything with a user.
  auto &&[assoc1, a0] = MakeInsert("association", {"0", "1", "5"});
  auto &&[assoc2, a1] = MakeInsert("association", {"1", "1", "0"});

  EXPECT_UPDATE(Execute(assoc1, &conn), 2);
  EXPECT_UPDATE(Execute(assoc2, &conn), 2);

  // Validate anon on forget for user with id 5.
  std::string forget = MakeGDPRForget("user", "5");
  EXPECT_UPDATE(Execute(forget, &conn), 3);
  db->BeginTransaction(false);
  EXPECT_EQ(db->GetShard("grps", SN("admin", "0")), (V{grow1, grow2}));
  EXPECT_EQ(db->GetShard("files", SN("admin", "0")), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN("admin", "0")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "0")), (V{grow2}));
  EXPECT_EQ(db->GetShard("grps", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("grps", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetShard("files", SN("user", "0")),
            (V{frow1_anon, frow2_anon}));
  EXPECT_EQ(db->GetShard("files", SN("user", "5")), (V{}));
  EXPECT_EQ(db->GetShard("files", SN(DEFAULT_SHARD, DEFAULT_SHARD)), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN("user", "0")), (V{}));
  EXPECT_EQ(db->GetShard("fassoc", SN("user", "5")), (V{}));
  db->RollbackTransaction();
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
