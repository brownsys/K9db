#include "tests/common.h"

TEST(E2ECorrectnessTest, MedicalChat) {
  tests::RunTest("tests/data/medical/queries");
}

int main(int argc, char **argv) {
  return tests::TestingMain(argc, argv, "medical",
                            "tests/data/medical/schema.sql", "",
                            "tests/data/medical/inserts.sql");
}
