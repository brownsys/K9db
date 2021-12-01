#include "tests/common.h"

TEST(E2ECorrectnessTest, DELFECommerce) {
  tests::RunTest("tests/data/delf_ecommerce/queries");
}

int main(int argc, char **argv) {
  return tests::TestingMain(argc, argv, "delf_ecommerce", 2,
                            "tests/data/delf_ecommerce/schema.sql",
                            "tests/data/delf_ecommerce/inserts.sql");
}
