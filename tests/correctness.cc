#include <fstream>
#include <iostream>
#include <string>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "pelton/pelton.h"

#define DB_NAME "correctness_test"

namespace {

// Expects that two vectors are equal regardless of order
inline void EXPECT_EQ_MSET(std::vector<std::string> expected,
                           std::vector<std::string> actual, std::string query) {
  // sort to remove differences in order
  std::sort(actual.begin(), actual.end());
  std::sort(expected.begin(), expected.end());
  EXPECT_EQ(expected, actual) << "failed query was: " << query;
}

template <typename T>
std::string tostring(const T &x) {
  std::ostringstream os;
  os << x;
  return os.str();
}

struct TestInputs {
  std::vector<std::string> creates;
  std::vector<std::string> inserts;
  std::vector<std::string> updates;
  std::vector<std::string> deletes;
  std::vector<std::pair<std::string, std::string>> flows;
  std::vector<std::string> queries;
  std::vector<std::vector<std::string>> expected_outputs;
};

};  // namespace

// Read MySql configurations.
// Command line flags.
DEFINE_string(db_username, "root", "MariaDB username to connect with");
DEFINE_string(db_password, "password", "MariaDB pwd to connect with");

static std::string *db_username;
static std::string *db_password;

void DropDatabase() {
  sql::ConnectOptionsMap connection_properties;
  connection_properties["hostName"] = "localhost";
  connection_properties["userName"] = *db_username;
  connection_properties["password"] = *db_password;

  sql::Driver *driver = sql::mariadb::get_driver_instance();
  std::unique_ptr<sql::Connection> conn =
      std::unique_ptr<sql::Connection>(driver->connect(connection_properties));
  std::unique_ptr<sql::Statement> stmt =
      std::unique_ptr<sql::Statement>(conn->createStatement());

  stmt->execute("DROP DATABASE IF EXISTS " DB_NAME);
}

void ReadInputs(const std::string &schema_file, const std::string &queries_file,
                const std::string &inserts_file,
                const std::string &expected_output_file, TestInputs *inputs) {
  // * process schema (input file 1)
  std::ifstream schema(schema_file);
  std::string line;
  if (schema.is_open()) {
    std::string table = "";
    while (std::getline(schema, line)) {
      // if line is empty or commented out, skip it
      if (line == "" || line.find("--skip--") != std::string::npos) continue;

      // append to string until ';' marking the end of a CREATE statement
      table.append(line);

      // if we've reached the end of a CREATE statement, add to CREATES
      if (line.find(";") != std::string::npos) {
        inputs->creates.push_back(table);
        table = "";
      }
    }
    schema.close();
  } else {
    LOG(FATAL) << "couldn't open schema file " << schema_file;
  }

  // * process queries (input file 2)
  std::ifstream queries(queries_file);
  if (queries.is_open()) {
    while (std::getline(queries, line)) {
      if (line == "" || line.find("--skip--") != std::string::npos) continue;
      if (line.find("VIEW") != std::string::npos) {
        std::vector<std::string> split_on_space;
        std::istringstream iss(line);
        for (std::string s; iss >> s;) split_on_space.push_back(s);
        // key of this pair is the name of the view at index 2
        inputs->flows.push_back(std::make_pair(split_on_space[2], line));
      } else if (line.find("INSERT") != std::string::npos) {
        inputs->inserts.push_back(line);
      } else if (line.find("SELECT") != std::string::npos) {
        inputs->queries.push_back(line);
      } else if (line.find("DELETE") != std::string::npos) {
        inputs->deletes.push_back(line);
      } else if (line.find("UPDATE") != std::string::npos) {
        inputs->updates.push_back(line);
      }
    }
    queries.close();
  } else {
    LOG(FATAL) << "couldn't open queries file " << queries_file;
  }

  // * process inserts (input file 3)
  std::ifstream inserts(inserts_file);
  if (inserts.is_open()) {
    LOG(INFO) << "inserts file opened";
    while (std::getline(inserts, line)) {
      if (line == "" || line.find("--skip--") != std::string::npos) continue;
      if (line.find("INSERT") != std::string::npos) {
        inputs->inserts.push_back(line);
      }
    }
    inserts.close();
  } else {
    LOG(FATAL) << "couldn't open inserts file " << inserts_file;
  }

  // * process expected results (input file 4)
  std::ifstream expected(expected_output_file);
  if (expected.is_open()) {
    LOG(INFO) << "expected results file open";
    // vector containing result for one query
    std::vector<std::string> expected_result;
    while (std::getline(expected, line)) {
      if (line == "" || line.find("--skip--") != std::string::npos) continue;
      // if ';' is found, we've reached the end of a single query's results
      if (line.find(";") != std::string::npos) {
        // add to the 2D vector of all query results
        inputs->expected_outputs.push_back(expected_result);
        // clear vector
        expected_result.clear();
        continue;
      }
      // '|' pre- and appended to match string formatting of pelton
      std::string format = "|";
      line.append(format);
      line.insert(0, 1, '|');
      expected_result.push_back(line);
    }
    expected.close();
  } else {
    LOG(FATAL) << "couldn't open expected results file "
               << expected_output_file;
  }
}

void RunTest(const std::string &schema_file, const std::string &query_file,
             const std::string &inserts_file,
             const std::string &expected_outputs_file) {
  TestInputs inputs;
  ReadInputs(schema_file, query_file, inserts_file, expected_outputs_file,
             &inputs);

  // Drop existing databases
  LOG(INFO) << "Dropping DB...";
  DropDatabase();

  // Open connection to sharder.
  pelton::Connection connection;
  pelton::initialize("", DB_NAME, *db_username, *db_password);
  pelton::open(&connection);
  // CHECK(pelton::exec(&connection, "SET echo;").ok());

  // Create all the tables.
  LOG(INFO) << "Create the tables ... ";
  for (std::string &create : inputs.creates) {
    CHECK(pelton::exec(&connection, create).ok());
  }

  // Add flows.
  LOG(INFO) << "Installing flows ... ";
  for (const auto &[name, query] : inputs.flows) {
    LOG(INFO) << name;
    CHECK(pelton::exec(&connection, query).ok());
  }

  // Insert data into the tables.
  LOG(INFO) << "Insert data into tables ... ";
  for (std::string &insert : inputs.inserts) {
    CHECK(pelton::exec(&connection, insert).ok());
  }

  // Updates
  LOG(INFO) << "Update data in tables ... ";
  for (std::string &update : inputs.updates) {
    CHECK(pelton::exec(&connection, update).ok());
  }

  // Deletes
  LOG(INFO) << "Delete data from tables ... ";
  for (std::string &del : inputs.deletes) {
    CHECK(pelton::exec(&connection, del).ok());
  }

  // * compare pelton to expected results

  LOG(INFO) << "Check flows and queries... ";
  // run each query
  size_t i = 0;
  for (const auto &query : inputs.queries) {
    // if we exceed the number of expected results
    if (i >= inputs.expected_outputs.size()) {
      break;
    }
    if (inputs.expected_outputs.size() == 0) {
      LOG(FATAL) << "No expected results provided!";
    }
    auto status = pelton::exec(&connection, query);
    CHECK(status.ok());

    // Print(std::move(status.value()));
    pelton::SqlResult &result = status.value();

    // vector to hold query results.
    std::vector<std::string> query_actual;

    // add records to query_results
    std::unique_ptr<pelton::SqlResultSet> resultset = result.NextResultSet();
    for (pelton::Record &record : *resultset) {
      // TODO(babman): fix pelton outputing all records as negative.
      record.SetPositive(true);
      query_actual.push_back(tostring(record));
    }

    // add schema e.g. |id(INT, KEY)| to actual & expected
    // query_actual.push_back(tostring(result.GetSchema()));
    // EXPECTED[i].push_back(tostring(result.GetSchema()));

    if (query.find("ORDER BY") != std::string::npos) {
      // ordered comparison
      EXPECT_EQ(inputs.expected_outputs[i], query_actual)
          << "failed query was: " << query;
    } else {
      // orderless comparison
      EXPECT_EQ_MSET(inputs.expected_outputs[i], query_actual, tostring(query));
    }
    i++;
  }

  // Close connection.
  pelton::close(&connection);
  pelton::shutdown(false);
}

void RunLobstersTest(size_t query_id) {
  char e_str[100];
  char q_str[100];
  snprintf(q_str, sizeof(q_str), "tests/data/lobsters_q%lu.sql", query_id);
  snprintf(e_str, sizeof(e_str), "tests/data/q%lu.txt", query_id);
  RunTest(std::string("tests/data/lobsters_schema_simplified.sql"),
          std::string(q_str), std::string("tests/data/lobsters_inserts.sql"),
          std::string(e_str));
}

TEST(E2ECorrectnessTest, MedicalChat) {
  RunTest(std::string("tests/data/medical_chat_schema.sql"),
          std::string("tests/data/medical_chat_queries.sql"),
          std::string("tests/data/medical_chat_inserts.sql"),
          std::string("tests/data/medical_chat_expected.txt"));
}

TEST(E2ECorrectnessTest, LobstersQ1) { RunLobstersTest(1); }
TEST(E2ECorrectnessTest, LobstersQ2) { RunLobstersTest(2); }
TEST(E2ECorrectnessTest, LobstersQ3) { RunLobstersTest(3); }
TEST(E2ECorrectnessTest, LobstersQ4) { RunLobstersTest(4); }
TEST(E2ECorrectnessTest, LobstersQ5) { RunLobstersTest(5); }
TEST(E2ECorrectnessTest, LobstersQ6) { RunLobstersTest(6); }
TEST(E2ECorrectnessTest, LobstersQ7) { RunLobstersTest(7); }
TEST(E2ECorrectnessTest, LobstersQ8) { RunLobstersTest(8); }
TEST(E2ECorrectnessTest, LobstersQ9) { RunLobstersTest(9); }
TEST(E2ECorrectnessTest, LobstersQ10) { RunLobstersTest(10); }
TEST(E2ECorrectnessTest, LobstersQ11) { RunLobstersTest(11); }
TEST(E2ECorrectnessTest, LobstersQ12) { RunLobstersTest(12); }
TEST(E2ECorrectnessTest, LobstersQ13) { RunLobstersTest(13); }
TEST(E2ECorrectnessTest, LobstersQ14) { RunLobstersTest(14); }
TEST(E2ECorrectnessTest, LobstersQ15) { RunLobstersTest(15); }
TEST(E2ECorrectnessTest, LobstersQ16) { RunLobstersTest(16); }
TEST(E2ECorrectnessTest, LobstersQ17) { RunLobstersTest(17); }
TEST(E2ECorrectnessTest, LobstersQ18) { RunLobstersTest(18); }
TEST(E2ECorrectnessTest, LobstersQ19) { RunLobstersTest(19); }
TEST(E2ECorrectnessTest, LobstersQ20) { RunLobstersTest(20); }
TEST(E2ECorrectnessTest, LobstersQ21) { RunLobstersTest(21); }
TEST(E2ECorrectnessTest, LobstersQ22) { RunLobstersTest(22); }
TEST(E2ECorrectnessTest, LobstersQ23) { RunLobstersTest(23); }
TEST(E2ECorrectnessTest, LobstersQ24) { RunLobstersTest(24); }
TEST(E2ECorrectnessTest, LobstersQ25) { RunLobstersTest(25); }
TEST(E2ECorrectnessTest, LobstersQ26) { RunLobstersTest(26); }
TEST(E2ECorrectnessTest, LobstersQ27) { RunLobstersTest(27); }
TEST(E2ECorrectnessTest, LobstersQ28) { RunLobstersTest(28); }
// TODO(malte): this test fails currently, but will pass when we
// support nested materialized view definitions
// TEST(E2ECorrectnessTest, LobstersQ29) { RunLobstersTest(29); }
TEST(E2ECorrectnessTest, LobstersQ30) { RunLobstersTest(30); }
TEST(E2ECorrectnessTest, LobstersQ31) { RunLobstersTest(31); }
TEST(E2ECorrectnessTest, LobstersQ32) { RunLobstersTest(32); }
TEST(E2ECorrectnessTest, LobstersQ33) { RunLobstersTest(33); }
TEST(E2ECorrectnessTest, LobstersQ34) { RunLobstersTest(34); }
TEST(E2ECorrectnessTest, LobstersQ35) { RunLobstersTest(35); }
TEST(E2ECorrectnessTest, LobstersQ36) { RunLobstersTest(36); }

int main(int argc, char **argv) {
  // Command line arugments and help message.
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  db_username = new std::string(FLAGS_db_username);
  db_password = new std::string(FLAGS_db_password);

  // Initialize Google’s logging library.
  google::InitGoogleLogging("correctness");

  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
