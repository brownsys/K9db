#include "tests/common.h"

TEST(E2ECorrectnessTest, LobstersQ1) {
  tests::RunTest("tests/data/lobsters/q1");
}
TEST(E2ECorrectnessTest, LobstersQ2) {
  tests::RunTest("tests/data/lobsters/q2");
}
TEST(E2ECorrectnessTest, LobstersQ3) {
  tests::RunTest("tests/data/lobsters/q3");
}
TEST(E2ECorrectnessTest, LobstersQ4) {
  tests::RunTest("tests/data/lobsters/q4");
}
TEST(E2ECorrectnessTest, LobstersQ5) {
  tests::RunTest("tests/data/lobsters/q5");
}
TEST(E2ECorrectnessTest, LobstersQ6) {
  tests::RunTest("tests/data/lobsters/q6");
}
TEST(E2ECorrectnessTest, LobstersQ7) {
  tests::RunTest("tests/data/lobsters/q7");
}
TEST(E2ECorrectnessTest, LobstersQ8) {
  tests::RunTest("tests/data/lobsters/q8");
}
TEST(E2ECorrectnessTest, LobstersQ9) {
  tests::RunTest("tests/data/lobsters/q9");
}
TEST(E2ECorrectnessTest, LobstersQ10) {
  tests::RunTest("tests/data/lobsters/q10");
}
TEST(E2ECorrectnessTest, LobstersQ11) {
  tests::RunTest("tests/data/lobsters/q11");
}
TEST(E2ECorrectnessTest, LobstersQ12) {
  tests::RunTest("tests/data/lobsters/q12");
}
TEST(E2ECorrectnessTest, LobstersQ13) {
  tests::RunTest("tests/data/lobsters/q13");
}
TEST(E2ECorrectnessTest, LobstersQ14) {
  tests::RunTest("tests/data/lobsters/q14");
}
TEST(E2ECorrectnessTest, LobstersQ15) {
  tests::RunTest("tests/data/lobsters/q15");
}
TEST(E2ECorrectnessTest, LobstersQ16) {
  tests::RunTest("tests/data/lobsters/q16");
}
TEST(E2ECorrectnessTest, LobstersQ17) {
  tests::RunTest("tests/data/lobsters/q17");
}
TEST(E2ECorrectnessTest, LobstersQ18) {
  tests::RunTest("tests/data/lobsters/q18");
}
TEST(E2ECorrectnessTest, LobstersQ19) {
  tests::RunTest("tests/data/lobsters/q19");
}
TEST(E2ECorrectnessTest, LobstersQ20) {
  tests::RunTest("tests/data/lobsters/q20");
}
TEST(E2ECorrectnessTest, LobstersQ21) {
  tests::RunTest("tests/data/lobsters/q21");
}
TEST(E2ECorrectnessTest, LobstersQ22) {
  tests::RunTest("tests/data/lobsters/q22");
}
TEST(E2ECorrectnessTest, LobstersQ23) {
  tests::RunTest("tests/data/lobsters/q23");
}
TEST(E2ECorrectnessTest, LobstersQ24) {
  tests::RunTest("tests/data/lobsters/q24");
}
TEST(E2ECorrectnessTest, LobstersQ25) {
  tests::RunTest("tests/data/lobsters/q25");
}
TEST(E2ECorrectnessTest, LobstersQ26) {
  tests::RunTest("tests/data/lobsters/q26");
}
TEST(E2ECorrectnessTest, LobstersQ27) {
  tests::RunTest("tests/data/lobsters/q27");
}
TEST(E2ECorrectnessTest, LobstersQ28) {
  tests::RunTest("tests/data/lobsters/q28");
}
TEST(E2ECorrectnessTest, LobstersQ29) {
  tests::RunTest("tests/data/lobsters/q29");
}
TEST(E2ECorrectnessTest, LobstersQ30) {
  tests::RunTest("tests/data/lobsters/q30");
}
TEST(E2ECorrectnessTest, LobstersQ31) {
  tests::RunTest("tests/data/lobsters/q31");
}
TEST(E2ECorrectnessTest, LobstersQ32) {
  tests::RunTest("tests/data/lobsters/q32");
}
TEST(E2ECorrectnessTest, LobstersQ33) {
  tests::RunTest("tests/data/lobsters/q33");
}
TEST(E2ECorrectnessTest, LobstersQ34) {
  tests::RunTest("tests/data/lobsters/q34");
}
TEST(E2ECorrectnessTest, LobstersQ35) {
  tests::RunTest("tests/data/lobsters/q35");
}
TEST(E2ECorrectnessTest, LobstersQ36) {
  tests::RunTest("tests/data/lobsters/q36");
}

int main(int argc, char **argv) {
  return tests::TestingMain(
      argc, argv, "lobsters", "experiments/lobsters/schema.sql",
      "experiments/lobsters/views.sql", "tests/data/lobsters/data.sql");
}
