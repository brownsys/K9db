#include "tests/common.h"

TEST(E2ECorrectnessTest, DELFMedicalChatRetain) {
  tests::RunTest("tests/data/delf_medical_retain/queries");
}

int main(int argc, char **argv) {
  return tests::TestingMain(argc, argv, "delf_medical_retain", 2,
                            "tests/data/delf_medical_retain/schema.sql",
                            "tests/data/delf_medical_retain/inserts.sql");
}
