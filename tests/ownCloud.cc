
#include "glog/logging.h"
#include "tests/common.h"


std::string data_file(const std::string &s) { return "tests/data/ownCloud/" + s; }

class OwnCloud : public tests::CleanDatabaseFixture {};

TEST_F(OwnCloud, Bug1ProbablySharding) {
  tests::RunTest(data_file("bug_1_probably_sharding"));
}


int main(int argc, char **argv) {
  return tests::TestingMainFixture(argc, argv, "ownCloud", 2,
                                    data_file("schema.sql").c_str(),
                                    data_file("inserts.sql").c_str());
}