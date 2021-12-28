
#include "tests/common.h"
#include "glog/logging.h"

// Features / things to test

std::string data_file(const std::string &s) {
    return "tests/data/varown/" + s;
}

// ================= OWNS =================

// Storage: A resource that is variably shared shows up a users muDB's
TEST(Varown, Storage1) {
    // FIXME: In the test files it says "update # = 0" for the insertion of the
    // pointed-to resource, however that should be 1, obviously. Aka the update count 
    // for sharded insertion of variable owner resourecs is incorrect at the moment.
    tests::RunTest(data_file("storage1"));
}
// Storage 2: A resource that has multiple owners shows up in both users muDB's

// [Delayed] Movement: A resource that already exists is moved to the target user shard

// Point Lookup: A variably owned resource can be looked up normally

// Resource Deletes: A point delete delets all copies of a variably owned resource

// Relationship Deletes: Deleting the connecting row also deletes the pointed-to resource

// Also test with non-integer primary key (especially for the data subject)

// Selecting all records from the target table does not return duplicates.

// ================= ACCESSES =================

// An accesible resource shows up in a GDPR GET for the user




int main(int argc, char ** argv) {
    return tests::TestingMain(argc, argv, "varown", 1, data_file("schema.sql").c_str());
}