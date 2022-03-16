#include "tests/common.h"

TEST(E2ECorrectnessTest, Shuup) {
  tests::RunTest("tests/data/shuup/queries");
}

int main(int argc, char **argv) {
  return tests::TestingMain(argc, argv, "shuup", 2,
                            "tests/data/shuup/schema.sql",
                            "tests/data/shuup/inserts.sql");
}
