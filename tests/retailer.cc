#include "tests/common.h"

TEST(E2ECorrectnessTest, Retailer) {
  tests::RunTest("tests/data/retailer/queries");
}

int main(int argc, char **argv) {
  return tests::TestingMain(argc, argv, "retailer", 2,
                            "tests/data/retailer/schema.sql",
                            "tests/data/retailer/inserts.sql");
}
