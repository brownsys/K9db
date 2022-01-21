#ifndef TESTS_COMMON_H_
#define TESTS_COMMON_H_

#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/pelton.h"

namespace tests {

// This function is responsible for setting up, running, and shutting down
// any correctness tests defined in enclosing .cc files.
// This function takes in the argc and argv arguments passed to main from
// the command line, as well as any number of file paths that contain
// SQL commands to use to initialize the DB prior to testing.
int TestingMain(int argc, char **argv, const std::string &testname,
                size_t file_count, ...);

// Run tests  with per-test setup and tear-down using a fixture. This means that
// you'll have a clean database for every single test (with the schema you
// provide to this function loaded). To use this feature you can define your
// test fixture as a C++ class that **publicly** inherits from
// `tests::CleanDatabaseFixture` and then using the `TEST_F` macro with your
// fixture class as argument to define tests instead of `TEST`.
//
// What this fixture does is it deletes the database after every test and
// reloads your schema and the initial inserts you defined. This should make it
// easier to define multiple tests in a file, especially if they use GDPR
// queries, and have them not interfere.
int TestingMainFixture(int argc, char **argv, const std::string &testname,
                       size_t file_count, ...);

// Constitutes a test that checks that pelton produces a matching output
// for the given queries.
void RunTest(const std::string &query_file_prefix);

// See documentation for `TestingMainFixture`
class CleanDatabaseFixture : public ::testing::Test {
 public:
  static void InitStatics(const std::string &db_name,
                          const std::vector<std::string> &file_path_args);

 protected:
  void SetUp() override;
  void TearDown() override;
  static const std::string &db_name();
  static const std::vector<std::string> &file_path_args();

 private:
  static std::optional<std::string> _db_name;
  static std::optional<std::vector<std::string>> _file_path_args;
};

}  // namespace tests

#endif  // TESTS_COMMON_H_
