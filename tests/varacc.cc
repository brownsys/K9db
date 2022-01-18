
#include "glog/logging.h"
#include "tests/common.h"

// Features / things to test

std::string data_file(const std::string &s) { return "tests/data/varacc/" + s; }

class Varacc : public tests::CleanDatabaseFixture {};

// ================= ACCESSES =================

// An accesible resource shows up in a GDPR GET for the user

// TEST_F(Varacc, Access) {
//   tests::RunTest(data_file("access"));
// }

// An accessible resource is not deleted if the accessor forgets their data

TEST_F(Varacc, GDPRForget) { tests::RunTest(data_file("forget")); }

int main(int argc, char **argv) {
  try {
    return tests::TestingMainFixture(argc, argv, "varacc", 2,
                                     data_file("schema.sql").c_str(),
                                     data_file("inserts.sql").c_str());
  } catch (std::exception &e) {
    LOG(FATAL) << "Exception " << e.what();
  }
}