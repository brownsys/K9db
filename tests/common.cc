#include "tests/common.h"

#include <stdarg.h>

#include <algorithm>
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
DEFINE_string(db_username, "root", "MariaDB username to connect with");
DEFINE_string(db_password, "password", "MariaDB pwd to connect with");
DEFINE_bool(echo, false, "whether to echo commands before executing them");

namespace {

// Pelton connection lives from InitializeDatabase(...) until TearDown(...).
pelton::Connection *connection;

// Turn record into string in format similar to rows in expected files.
template <typename T>
std::string ToString(const T &x) {
  std::ostringstream os;
  os << x;
  return os.str();
}

// Drop the database (if it exists).
void DropDatabase(const std::string &db_name) {
  sql::ConnectOptionsMap connection_properties;
  connection_properties["hostName"] = "localhost";
  connection_properties["userName"] = FLAGS_db_username;
  connection_properties["password"] = FLAGS_db_password;

  sql::Driver *driver = sql::mariadb::get_driver_instance();
  std::unique_ptr<sql::Connection> conn =
      std::unique_ptr<sql::Connection>(driver->connect(connection_properties));
  std::unique_ptr<sql::Statement> stmt =
      std::unique_ptr<sql::Statement>(conn->createStatement());

  stmt->execute("DROP DATABASE IF EXISTS " + db_name);
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
void InitializeDatabase(const std::string &db_name, size_t file_count,
                        va_list file_path_args) {
  const std::string &db_username = FLAGS_db_username;
  const std::string &db_password = FLAGS_db_password;

  // Drop test database if it exists.
  LOG(INFO) << "Dropping DB " << db_name << "...";
  DropDatabase(db_name);

  // Create and open a connection to pelton.
  connection = new pelton::Connection();
  CHECK(pelton::initialize("", db_name, db_username, db_password));
  CHECK(pelton::open(connection));

  // Set echo if specified by cmd flags.
  if (FLAGS_echo) {
    CHECK(pelton::exec(connection, "SET echo;").ok());
  }

  // Create all the tables.
  for (size_t i = 0; i < file_count; i++) {
    const char *file_path = va_arg(file_path_args, const char *);
    LOG(INFO) << "Executing file " << file_path;
    auto commands = ReadSQL(file_path);
    for (const std::string &command : commands) {
      CHECK(pelton::exec(connection, command).ok());
    }
  }
}

// Clean up after testing is complete.
void TearDown(const std::string &db_name) {
  // Close connection to pelton.
  pelton::close(connection);
  delete connection;

  // Drop the test database.
  LOG(INFO) << "Dropping DB " << db_name << "...";
  DropDatabase(db_name);
}

}  // namespace

int TestingMain(int argc, char **argv, const std::string &testname,
                size_t file_count, ...) {
  // Setup va list containing variadic file paths.
  va_list file_path_args;
  va_start(file_path_args, file_count);

  // Command line arugments and help message.
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  const std::string &db_name = testname + "_test";

  // Initialize Google’s logging library.
  google::InitGoogleLogging(testname.c_str());
  ::testing::InitGoogleTest(&argc, argv);

  // Setup the schema (once).
  InitializeDatabase(db_name, file_count, file_path_args);

  // Run tests.
  auto result = RUN_ALL_TESTS();

  // Tear down database.
  TearDown(db_name);

  // Clean up the va list.
  va_end(file_path_args);

  return result;
}

// Constitutes a test that checks that pelton produces a matching output
// for the given queries.
void RunTest(const std::string &query_file_prefix) {
  auto queries = ReadSQL(query_file_prefix + ".sql");
  auto results = ReadExpected(query_file_prefix + ".txt");

  // Run each query and compare its output to expected.
  for (size_t i = 0; i < queries.size(); i++) {
    const std::string &query = queries.at(i);
    std::vector<std::string> &expected = results.at(i);

    // Run query.
    auto status = pelton::exec(connection, query);
    EXPECT_TRUE(status.ok()) << status.status();

    // Store output.
    std::vector<std::string> actual;
    pelton::SqlResult &result = status.value();
    std::unique_ptr<pelton::SqlResultSet> resultset = result.NextResultSet();
    for (pelton::Record &record : *resultset) {
      record.SetPositive(true);
      actual.push_back(ToString(record));
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

}  // namespace tests
