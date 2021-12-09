#include "tests/common.h"

TEST(E2ECorrectnessTest, DELFMedicalChatAnonymize) {
  tests::RunTest("tests/data/delf_medical_anonymize/queries");
}

int main(int argc, char **argv) {
  return tests::TestingMain(argc, argv, "delf_medical_anonymize", 2,
                            "tests/data/delf_medical_anonymize/schema.sql",
                            "tests/data/delf_medical_anonymize/inserts.sql");
}
