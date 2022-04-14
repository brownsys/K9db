#include "tests/common.h"

TEST(E2ECorrectnessTest, MedicalChatNestedViews) {
  tests::RunTest("tests/data/medical_nested_views/queries");
}

int main(int argc, char **argv) {
  return tests::TestingMain(
      argc, argv, "medical_nested_views", 3,
      "tests/data/medical_nested_views/schema.sql",
      "tests/data/medical_nested_views/inserts.sql",
      "tests/data/medical_nested_views/views.sql");
}
