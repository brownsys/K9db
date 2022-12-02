#include "glog/logging.h"
#include "pelton/shards/state.h"
#include "tests/common.h"

// Features / things to test

std::string data_file(const std::string &s) { return "tests/data/varown/" + s; }

class Varown : public tests::CleanDatabaseFixture {};

std::vector<pelton::dataflow::Record> SelectTFromDefaultDB(int64_t id) {
  auto *instance = tests::GetPeltonInstance();
  std::vector<pelton::sql::KeyPair> vec;
  vec.emplace_back(pelton::util::ShardName(DEFAULT_SHARD, DEFAULT_SHARD),
                   pelton::sqlast::Value(id));
  instance->session->BeginTransaction();
  auto result = instance->session->GetDirect("t", 0, vec);
  instance->session->RollbackTransaction();
  return result;
}

// ================= OWNS =================

// Storage: A resource that shows up in owning user(s) shard.
TEST_F(Varown, Storage2) { tests::RunTest(data_file("storage")); }

// Point Lookup: A variably owned resource can be looked up normally
TEST_F(Varown, Lookup) { tests::RunTest(data_file("lookup")); }

// Resource Deletes: A point delete delets all copies of a variably owned
// resource
TEST_F(Varown, Delete) { tests::RunTest(data_file("delete")); }

// Relationship Deletes: Deleting the connecting row also deletes the pointed-to
// resource Commented out because of record-type error
TEST_F(Varown, DeleteRel1) { tests::RunTest(data_file("delete_rel1")); }

// Relationship Deletes: Deleting the connecting row deletes the pointed-to
// resource only for the user mentioned in that row Commented out because of
// record-type error
TEST_F(Varown, DeleteRel2) { tests::RunTest(data_file("delete_rel2")); }

// Also test with non-integer primary key (especially for the data subject)

// Selecting all records from the target table does not return duplicates.
TEST_F(Varown, SelectStar) { tests::RunTest(data_file("select_star")); }

// Insert resource then association: resource moves from default shard to user.
TEST_F(Varown, NaturalInsertOrder) {
  tests::RunTest(data_file("natural_insert_order"));
  EXPECT_EQ(SelectTFromDefaultDB(2000).size(), 0);
}

// Insert another association for an owned resource: resource copied from
// previous user to new user.
TEST_F(Varown, CopyFromNonDefault) {
  tests::RunTest(data_file("copy_from_non_default"));
}

// Deleting last association moves owned resource to default shard.
TEST_F(Varown, DeleteMove) {
  tests::RunTest(data_file("delete_move"));
  EXPECT_EQ(SelectTFromDefaultDB(2000).size(), 1);
}

// Deleting association keeps resource unmoved if other associations exists.
TEST_F(Varown, DeleteNoMove) {
  tests::RunTest(data_file("delete_no_move"));
  EXPECT_EQ(SelectTFromDefaultDB(1005).size(), 0);
}

int main(int argc, char **argv) {
  try {
    return tests::TestingMainFixture(argc, argv, "varown", 2,
                                     data_file("schema.sql").c_str(),
                                     data_file("inserts.sql").c_str());
  } catch (std::exception &e) {
    LOG(FATAL) << "Exception " << e.what();
  }
}
