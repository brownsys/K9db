#include "tests/common.h"

TEST(E2ECorrectnessTest, WholeTables) {
  tests::RunTest("tests/data/websubmit/whole_tables");
}
TEST(E2ECorrectnessTest, GDPRGet) {
  tests::RunTest("tests/data/websubmit/gdpr_get");
}
TEST(E2ECorrectnessTest, GDPRForget) {
  tests::RunTest("tests/data/websubmit/gdpr_forget");
}

int main(int argc, char **argv) {
  return tests::TestingMain(
      argc, argv, "websubmit", 3, "tests/data/websubmit/schema.sql",
      "tests/data/websubmit/views.sql", "tests/data/websubmit/inserts.sql");
}
