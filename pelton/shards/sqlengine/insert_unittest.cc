#include "pelton/shards/sqlengine/insert.h"

#include <string>
#include <unordered_set>
#include <vector>

#include "glog/logging.h"
#include "pelton/shards/sqlengine/tests_helpers.h"

namespace pelton {
namespace shards {
namespace sqlengine {

/*
 * The tests!
 */

// Define a fixture that manages a pelton connection.
PELTON_FIXTURE(InsertTest);

TEST_F(InsertTest, UnshardedTable) {
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
  auto &&[stmt3, row3] = MakeInsert("user", {"2", "'u3'"});
  auto &&[stmt4, row4] = MakeInsert("user", {"5", "'u10'"});

  EXPECT_UPDATE(Execute(stmt1, &conn), 1);
  EXPECT_UPDATE(Execute(stmt2, &conn), 1);
  EXPECT_UPDATE(Execute(stmt3, &conn), 1);
  EXPECT_UPDATE(Execute(stmt4, &conn), 1);

  // Validate insertions.
  sql::SqlResultSet result = db->GetShard("user", DEFAULT_SHARD);
  EXPECT_EQ(result, (std::vector<std::string>{row1, row2, row3, row4}));
}

TEST_F(InsertTest, Datasubject) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});

  // Make a pelton connection.
  Connection conn = CreateConnection();
  sql::AbstractConnection *db = conn.state->Database();

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

  // Validate insertions.
  EXPECT_EQ(db->GetShard("user", "0"), (std::vector<std::string>{row1}));
  EXPECT_EQ(db->GetShard("user", "1"), (std::vector<std::string>{row2}));
  EXPECT_EQ(db->GetShard("user", "2"), (std::vector<std::string>{row3}));
  EXPECT_EQ(db->GetShard("user", "5"), (std::vector<std::string>{row4}));
  EXPECT_EQ(db->GetShard("user", "10"), (std::vector<std::string>{}));
  EXPECT_EQ(db->GetShard("user", DEFAULT_SHARD), (std::vector<std::string>{}));
}

TEST_F(InsertTest, DirectTable) {
  // Parse create table statements.
  std::string usr = MakeCreate("user", {"id" I PK, "PII_name" STR});
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

  // Validate insertions.
  EXPECT_EQ(db->GetShard("addr", "0"), (std::vector<std::string>{row1, row2}));
  EXPECT_EQ(db->GetShard("addr", "5"), (std::vector<std::string>{row3}));
  EXPECT_EQ(db->GetShard("addr", "10"), (std::vector<std::string>{}));
  EXPECT_EQ(db->GetShard("addr", DEFAULT_SHARD), (std::vector<std::string>{}));
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
