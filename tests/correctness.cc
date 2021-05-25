#include <fstream>
#include <iostream>
#include <string>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "pelton/pelton.h"
#include "pelton/util/perf.h"

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

  // * process inserts (input file 4)
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

  // * process expected results (input file 3)
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
  // Initialize Google’s logging library.
  google::InitGoogleLogging("correctness");

  // Read MySql configurations.
  const std::string &db_username = "root";
  const std::string &db_password = "password";

  TestInputs inputs;
  ReadInputs(schema_file, query_file, inserts_file, expected_outputs_file,
             &inputs);

  // * run schema and queries using pelton

  pelton::perf::Start("all");

  // Open connection to sharder.
  pelton::Connection connection;
  pelton::open("", db_username, db_password, &connection);
  CHECK(pelton::exec(&connection, "SET echo;").ok());

  // Create all the tables.
  LOG(INFO) << "Create the tables ... ";
  for (std::string &create : inputs.creates) {
    std::cout << std::endl;
    CHECK(pelton::exec(&connection, create).ok());
  }

  // Add flows.
  LOG(INFO) << "Installing flows ... ";
  for (const auto &[name, query] : inputs.flows) {
    std::cout << name << std::endl;
    CHECK(pelton::exec(&connection, query).ok());
  }
  pelton::shutdown_planner();

  // Insert data into the tables.
  LOG(INFO) << "Insert data into tables ... ";
  for (std::string &insert : inputs.inserts) {
    std::cout << std::endl;
    CHECK(pelton::exec(&connection, insert).ok());
  }

  // Updates
  LOG(INFO) << "Update data in tables ... ";
  for (std::string &update : inputs.updates) {
    std::cout << std::endl;
    CHECK(pelton::exec(&connection, update).ok());
  }

  // Deletes
  LOG(INFO) << "Delete data from tables ... ";
  for (std::string &del : inputs.deletes) {
    std::cout << std::endl;
    CHECK(pelton::exec(&connection, del).ok());
  }

  // * compare pelton to expected results

  LOG(INFO) << "Check flows and queries... ";
  // run each query
  long unsigned int i = 0;
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
    for (const pelton::Record &record : result) {
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

  // Print performance profile.
  pelton::perf::End("all");
  pelton::perf::PrintAll();
}

void RunLobstersTest(size_t query_id) {
  char q_str[100];
  snprintf(q_str, 100, "tests/data/lobsters_q%lu.sql", query_id);
  RunTest(std::string("tests/data/lobsters_schema_simplified.sql"),
          std::string(q_str), std::string("tests/data/lobsters_inserts.sql"),
          std::string("tests/data/lobsters_expected.txt"));
}

TEST(E2ECorrectnessTest, MedicalChat) {
  RunTest(std::string("tests/data/medical_chat_schema.sql"),
          std::string("tests/data/medical_chat_queries.sql"),
          std::string("tests/data/medical_chat_inserts.sql"),
          std::string("tests/data/medical_chat_expected.txt"));
}

TEST(E2ECorrectnessTest, LobstersQ1) { RunLobstersTest(1); }
