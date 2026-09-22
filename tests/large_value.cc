#include "tests/common.h"

// Regression test: end-to-end INSERT/SELECT of a value larger than the
// old fixed 10000-byte plaintext buffer that Decrypt() used to allocate,
// exercised through the full k9db::exec() SQL layer (parsing, sharding,
// storage, encryption) rather than the encryption primitives directly.
TEST(E2ECorrectnessTest, LargeEncryptedValue) {
  tests::RunTest("tests/data/large_value/queries");
}

int main(int argc, char **argv) {
  return tests::TestingMain(argc, argv, "large_value", 2,
                            "tests/data/large_value/schema.sql",
                            "tests/data/large_value/inserts.sql");
}
