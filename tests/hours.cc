#include "tests/common.h"

TEST(E2ECorrectnessTest, Hours) {
  tests::RunTest("tests/data/hours/queries");
}

int main(int argc, char **argv) {
  return tests::TestingMain(
      argc, argv, "hours", 3, "tests/data/hours/schema.sql",
      "tests/data/hours/inserts.sql", "tests/data/hours/views.sql");
}
