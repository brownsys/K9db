#ifndef TESTS_COMMON_H_
#define TESTS_COMMON_H_

#include <string>

#include "gtest/gtest.h"
#include "pelton/pelton.h"

namespace tests {

pelton::Connection connection;

// This function is responsible for setting up, running, and shutting down
// any correctness tests defined in enclosing .cc files.
// This function takes in the argc and argv arguments passed to main from
// the command line, as well as any number of file paths that contain
// SQL commands to use to initialize the DB prior to testing.
int TestingMain(int argc, char **argv, const std::string &testname,
                size_t file_count, ...);
                
int TestingMainFixture(int argc, char **argv, const std::string &testname,
                       size_t file_count, ...);

// Constitutes a test that checks that pelton produces a matching output
// for the given queries.
void RunTest(const std::string &query_file_prefix);

class CleanDatabaseFixture : public ::testing::Test {
public:
    static void InitStatics(const std::string &db_name, const std::vector<std::string> &file_path_args);
protected:
    void SetUp() override;
    void TearDown() override;
    const static std::string &db_name();
    const static std::vector<std::string> &file_path_args();
private:
    static std::optional<std::string> _db_name;
    static std::optional<std::vector<std::string>> _file_path_args;
};

}  // namespace tests

#endif  // TESTS_COMMON_H_
