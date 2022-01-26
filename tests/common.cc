#include "tests/common.h"

#include <stdarg.h>

#include <algorithm>
// NOLINTNEXTLINE
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "pelton/pelton.h"

namespace tests {

// Command line flags.
DEFINE_bool(echo, false, "whether to echo commands before executing them");

namespace {

pelton::Connection connection;

// Turn record into string in format similar to rows in expected files.
template <typename T>
std::string ToString(const T &x) {
  std::ostringstream os;
  os << x;
  return os.str();
}

// Drop the database (if it exists).
void DropDatabase(const std::string &db_name) {
  std::filesystem::remove_all("/tmp/pelton/rocksdb/" + db_name);
}

// Read an SQL file one command at a time.
std::vector<std::string> ReadSQL(const std::string &file) {
  std::ifstream stream(file);
  if (!stream.is_open()) {
    LOG(FATAL) << "Could not open file '" << file << "'";
  }

  // Read the file one command at a time.
  // A command ends with a non-escaped new line character.
  std::string line;
  std::string command = "";
  std::vector<std::string> content;
  while (std::getline(stream, line)) {
    // if line is empty or commented out, skip it
    if (line == "" || (line[0] == '-' && line[1] == '-') || line[0] == '#') {
      continue;
    }

    // append line to command until reaching a non-escaped empty line.
    if (line[line.size() - 1] != '\\') {
      command.append(line);
      content.push_back(std::move(command));
      command = "";
    } else {
      line.pop_back();
      command.append(line);
    }
  }
  stream.close();
  return content;
}

// Read expected outputs for several queries from an expected output file.
std::vector<std::vector<std::string>> ReadExpected(const std::string &file) {
  std::ifstream stream(file);
  if (!stream.is_open()) {
    LOG(FATAL) << "Could not open file '" << file << "'";
  }

  // Each query has a vector of expected results (i.e. a vector of rows).
  std::string line;
  std::vector<std::string> current_query;
  std::vector<std::vector<std::string>> results;
  while (std::getline(stream, line)) {
    // Skip comments.
    if (line == "" || (line[0] == '-' && line[1] == '-') || line[0] == '#') {
      continue;
    }
    // ; indicates the end of the expected results for one query.
    if (line == ";") {
      results.push_back(std::move(current_query));
      current_query.clear();
      continue;
    }
    // Prepend and append '|' to match pelton's output format.
    current_query.push_back("|" + line + "|");
  }
  stream.close();
  return results;
}

// Setup the database for testing. This includes its schema, views, and content.
void InitializeDatabase(const std::string &db_name,
                        const std::vector<std::string> &file_path_args) {
  // Drop test database if it exists.
  LOG(INFO) << "Dropping DB " << db_name << "...";
  DropDatabase(db_name);

  // Create and open a connection to pelton.
  CHECK(pelton::initialize(3, true));
  CHECK(pelton::open(&connection, db_name));

  // Set echo if specified by cmd flags.
  if (FLAGS_echo) {
    CHECK(pelton::exec(&connection, "SET echo;").ok());
  }

  // Create all the tables.
  for (const auto &file_path : file_path_args) {
    LOG(INFO) << "Executing file " << file_path;
    auto commands = ReadSQL(file_path);
    for (const std::string &command : commands) {
      const auto status = pelton::exec(&connection, command);
      if (!status.ok()) {
        LOG(FATAL) << status.status();
      }
    }
  }
  LOG(INFO) << "Initialized database";
}

// Clean up after testing is complete.
void TearDown(const std::string &db_name, bool shutdown_jvm) {
  LOG(INFO) << "Closing connections...";
  // Close connection to pelton.
  CHECK(pelton::close(&connection));
  CHECK(pelton::shutdown(shutdown_jvm));

  // Drop the test database.
  LOG(INFO) << "Dropping DB " << db_name << "...";
  DropDatabase(db_name);
}

void TearDown(const std::string &db_name) { TearDown(db_name, true); }

int GenericTestingMain(bool use_fixture, int argc, char **argv,
                       const std::string &testname, size_t file_count,
                       va_list file_path_args) {
  std::vector<std::string> file_path_vec;
  for (size_t i = 0; i < file_count; i++) {
    const char *file_path = va_arg(file_path_args, const char *);
    file_path_vec.emplace_back(file_path);
  }

  // Command line arugments and help message.
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  const std::string &db_name = testname + "_test";

  // Initialize Google’s logging library.
  google::InitGoogleLogging(testname.c_str());
  ::testing::InitGoogleTest(&argc, argv);

  if (use_fixture)
    CleanDatabaseFixture::InitStatics(db_name, file_path_vec);
  else
    // Setup the schema (once).
    InitializeDatabase(db_name, file_path_vec);

  // Run tests.
  auto result = RUN_ALL_TESTS();

  // Tear down database.
  if (!use_fixture) TearDown(db_name);

  return result;
}

}  // namespace

int TestingMain(int argc, char **argv, const std::string &testname,
                size_t file_count, ...) {
  va_list file_path_args;
  va_start(file_path_args, file_count);
  int res = GenericTestingMain(false, argc, argv, testname, file_count,
                               file_path_args);

  // Clean up the va list.
  va_end(file_path_args);

  return res;
}

int TestingMainFixture(int argc, char **argv, const std::string &testname,
                       size_t file_count, ...) {
  va_list file_path_args;
  va_start(file_path_args, file_count);
  int res = GenericTestingMain(true, argc, argv, testname, file_count,
                               file_path_args);

  // Clean up the va list.
  va_end(file_path_args);

  return res;
}

// Constitutes a test that checks that pelton produces a matching output
// for the given queries.
void RunTest(const std::string &query_file_prefix) {
  auto queries = ReadSQL(query_file_prefix + ".sql");
  auto results = ReadExpected(query_file_prefix + ".txt");

  ASSERT_EQ(queries.size(), results.size());

  // Run each query and compare its output to expected.
  for (size_t i = 0; i < queries.size(); i++) {
    const std::string &query = queries.at(i);
    std::vector<std::string> &expected = results.at(i);

    VLOG(1) << "Running query: " << query;
    // Run query.
    auto status = pelton::exec(&connection, query);
    EXPECT_TRUE(status.ok()) << status.status() << query;

    // Store output.
    std::vector<std::string> actual;
    pelton::SqlResult &result = status.value();
    if (result.IsQuery()) {
      for (pelton::SqlResultSet &resultset : result.ResultSets()) {
        std::vector<pelton::Record> records = resultset.Vec();
        for (pelton::Record &record : records) {
          record.SetPositive(true);
          actual.push_back(ToString(record));
        }
      }
    } else if (result.IsUpdate()) {
      actual.push_back("|update # = " + std::to_string(result.UpdateCount()) +
                       "|");
    }

    // Check output vs expected.
    if (query.find("ORDER BY") != std::string::npos) {
      // Ordered comparison.
      EXPECT_EQ(expected, actual) << "failed ordered query: " << query;
    } else {
      // Orderless comparison.
      std::sort(actual.begin(), actual.end());
      std::sort(expected.begin(), expected.end());
      EXPECT_EQ(expected, actual) << "failed unordered query: " << query;
    }
  }
}

const std::string &CleanDatabaseFixture::db_name() {
  if (!CleanDatabaseFixture::_db_name) LOG(FATAL) << "DB name not initialized";
  return *CleanDatabaseFixture::_db_name;
}

const std::vector<std::string> &CleanDatabaseFixture::file_path_args() {
  if (!CleanDatabaseFixture::_file_path_args)
    LOG(FATAL) << "File path arguments not initialized";
  return *CleanDatabaseFixture::_file_path_args;
}

void CleanDatabaseFixture::InitStatics(
    const std::string &db_name,
    const std::vector<std::string> &file_path_args) {
  CleanDatabaseFixture::_db_name = db_name;
  CleanDatabaseFixture::_file_path_args = file_path_args;
}

void CleanDatabaseFixture::SetUp() {
  InitializeDatabase(this->db_name(), this->file_path_args());
}

void CleanDatabaseFixture::TearDown() {
  tests::TearDown(this->db_name(), false);
}

pelton::Connection *GetPeltonInstance() {
  return &connection;
}

std::optional<std::string> CleanDatabaseFixture::_db_name;
std::optional<std::vector<std::string>> CleanDatabaseFixture::_file_path_args;

}  // namespace tests
