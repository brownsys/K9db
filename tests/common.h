#ifndef TESTS_COMMON_H_
#define TESTS_COMMON_H_

#include <string>

#include "gtest/gtest.h"

namespace tests {

// This function is responsible for setting up, running, and shutting down
// any correctness tests defined in enclosing .cc files.
int TestingMain(int argc, char **argv, const std::string &testname,
                const std::string &schema_file, const std::string &views_file,
                const std::string &inserts_file);

// Constitutes a test that checks that pelton produces a matching output
// for the given queries.
void RunTest(const std::string &query_file_prefix);

}  // namespace tests

#endif  // TESTS_COMMON_H_
