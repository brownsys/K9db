#include "tests/common.h"

TEST(E2ECorrectnessTest, DELFMedicalChat) {
  tests::RunTest("tests/data/delf_medical/queries");
}

int main(int argc, char **argv) {
  return tests::TestingMain(argc, argv, "delf_medical", 2,
                            "tests/data/delf_medical/schema.sql",
                            "tests/data/delf_medical/inserts.sql");
}
