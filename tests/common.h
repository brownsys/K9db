#ifndef TESTS_COMMON_H_
#define TESTS_COMMON_H_

#include <string>

#include "gtest/gtest.h"

namespace tests {

// This function is responsible for setting up, running, and shutting down
// any correctness tests defined in enclosing .cc files.
// This function takes in the argc and argv arguments passed to main from
// the command line, as well as any number of file paths that contain
// SQL commands to use to initialize the DB prior to testing.
int TestingMain(int argc, char **argv, const std::string &testname, size_t file_count, ...);

// Constitutes a test that checks that pelton produces a matching output
// for the given queries.
void RunTest(const std::string &query_file_prefix);

}  // namespace tests

#endif  // TESTS_COMMON_H_
