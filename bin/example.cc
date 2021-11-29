#include <iostream>
#include <utility>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "pelton/pelton.h"
#include "pelton/util/perf.h"

DEFINE_string(db_username, "root", "MYSQL username to connect with");
DEFINE_string(db_password, "password", "MYSQL pwd to connect with");

namespace {

// CREATE TABLE queries.
std::vector<std::string> CREATES{
    // Students.
    "CREATE TABLE students ("
    "ID int,"
    "PII_Name text,"
    "PRIMARY KEY(ID)"
    ");",
    // Assignments.
    "CREATE TABLE assignments ("
    "ID int,"
    "Name text,"
    "PRIMARY KEY(ID)"
    ");",
    // Submissions.
    "CREATE TABLE submissions ("
    "ID int,"
    "student_id int,"
    "assignment_id int,"
    "ts int,"
    "PRIMARY KEY(ID),"
    "FOREIGN KEY (student_id) REFERENCES students(ID),"
    "FOREIGN KEY (assignment_id) REFERENCES assignments(ID)"
    ");",
    // Submissions index.
    "CREATE INDEX sind ON submissions(assignment_id);"};

// Inserts.
std::vector<std::string> INSERTS{
    "INSERT INTO assignments VALUES (1, 'assignment 1');",
    "INSERT INTO assignments VALUES (2, 'assignment 2');",
    "INSERT INTO students VALUES (1, 'Jerry');",
    "INSERT INTO students VALUES (2, 'Layne');",
    "INSERT INTO students VALUES (3, 'Sean');",
    "INSERT INTO submissions VALUES (1, 1, 1, 1);",
    "INSERT INTO submissions VALUES (2, 2, 1, 200);",
    "INSERT INTO submissions VALUES (3, 3, 1, 3);",
    "INSERT INTO submissions VALUES (4, 1, 1, 400);",
    "INSERT INTO submissions VALUES (5, 1, 2, 500);",
    "INSERT INTO submissions VALUES (6, 2, 2, 600);",
    "INSERT INTO submissions VALUES (7, 3, 2, 7);",
    "INSERT INTO submissions VALUES (8, 3, 2, 700);"};

// Updates.
std::vector<std::string> UPDATES{
    "UPDATE submissions SET ts = 2000 WHERE ts = 200;",
    "UPDATE submissions SET assignment_id = 1, ts = 1000 WHERE student_id = "
    "3;"};

// Deletes.
std::vector<std::string> DELETES{
    "DELETE FROM submissions WHERE ID = 6;",
    "DELETE FROM submissions WHERE student_id = 3;"};

// Flows.
std::vector<std::pair<std::string, std::string>> FLOWS{
    std::make_pair("all_rows",
                   "CREATE VIEW all_rows AS '\"SELECT * FROM submissions\"'"),
    std::make_pair("filter_row",
                   "CREATE VIEW filter_row AS "
                   "'\"SELECT * FROM submissions WHERE ts >= 100\"'"),
    std::make_pair("filter_row2",
                   "CREATE VIEW filter_row2 AS "
                   "'\"SELECT * FROM submissions WHERE ts < 100\"'"),
    std::make_pair("filter_row3",
                   "CREATE VIEW filter_row3 AS "
                   "'\"SELECT * FROM submissions WHERE ts >= 100 AND "
                   "assignment_id = 2 AND ID > 5\"'"),
    std::make_pair("union_flow",
                   "CREATE VIEW union_flow AS "
                   "'\"(SELECT * FROM submissions WHERE ts < 100) UNION "
                   "(SELECT * FROM submissions WHERE ts >= 100)\"'"),
    std::make_pair("join_flow",
                   "CREATE VIEW join_flow AS "
                   "'\"SELECT * from submissions INNER JOIN students ON "
                   "submissions.student_id = students.ID WHERE ts >= 100\"'"),
    std::make_pair(
        "limit_constant",
        "CREATE VIEW limit_constant AS "
        "'\"SELECT * from submissions ORDER BY ts LIMIT 2 OFFSET 5\"'"),
    std::make_pair("limit_variable",
                   "CREATE VIEW limit_variable AS "
                   "'\"SELECT * from submissions ORDER BY ts LIMIT ?\"'"),
    std::make_pair("sub_by_pk",
                   "CREATE VIEW sub_by_pk AS "
                   "'\"SELECT * from submissions WHERE ID = ?\"'")};

std::vector<std::string> FLOW_READS{
    "SELECT * FROM all_rows;",
    "SELECT * FROM filter_row;",
    "SELECT * FROM filter_row2;",
    "SELECT * FROM filter_row3;",
    "SELECT * FROM union_flow;",
    "SELECT * FROM join_flow;",
    "SELECT * FROM limit_constant;",
    "SELECT * FROM limit_variable WHERE ts > 100 LIMIT 2 OFFSET 1;",
    "SELECT * FROM sub_by_pk WHERE ID in (1, 4, 8, 100);",
    "SELECT * FROM sub_by_pk;"};

// Selects.
std::vector<std::string> QUERIES{
    "SELECT * FROM submissions;",
    "SELECT * FROM submissions WHERE student_id = 1;",
    "SELECT * FROM submissions WHERE assignment_id = 2;"};

// Printing query results.
void Print(pelton::SqlResult &&result) {
  if (result.IsStatement()) {
    std::cout << "Success: " << result.Success() << std::endl;
  } else if (result.IsUpdate()) {
    std::cout << "Affected rows: " << result.UpdateCount() << std::endl;
  } else if (result.IsQuery()) {
    while (result.HasResultSet()) {
      std::unique_ptr<pelton::SqlResultSet> resultset = result.NextResultSet();
      std::cout << resultset->GetSchema() << std::endl;
      for (const pelton::Record &record : *resultset) {
        std::cout << record << std::endl;
      }
    }
  }
}

}  // namespace

int main(int argc, char **argv) {
  // Command line arugments and help message.
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Initialize Google’s logging library.
  google::InitGoogleLogging("example");

  // Read MySql configurations.
  const std::string &db_username = FLAGS_db_username;
  const std::string &db_password = FLAGS_db_password;

  // Open connection to sharder.
  pelton::initialize(3);

  pelton::Connection connection;
  pelton::open(&connection, "exampledb", db_username, db_password);
  CHECK(pelton::exec(&connection, "SET echo;").ok());

  // Create all the tables.
  std::cout << "Create the tables ... " << std::endl;
  for (std::string &create : CREATES) {
    std::cout << std::endl;
    CHECK(pelton::exec(&connection, create).ok());
  }
  std::cout << std::endl;

  // Add flows.
  std::cout << "Installing flows ... " << std::endl;
  for (const auto &[name, query] : FLOWS) {
    std::cout << name << std::endl;
    CHECK(pelton::exec(&connection, query).ok());
  }
  pelton::shutdown_planner();
  std::cout << std::endl;

  // Insert some data into the tables.
  std::cout << "Insert data into tables ... " << std::endl;
  for (std::string &insert : INSERTS) {
    std::cout << std::endl;
    CHECK(pelton::exec(&connection, insert).ok());
  }
  std::cout << std::endl;

  // Read flow.
  std::cout << "Read flows ... " << std::endl;
  for (const auto &query : FLOW_READS) {
    std::cout << std::endl;
    auto status = pelton::exec(&connection, query);
    CHECK(status.ok());
    Print(std::move(status.value()));
  }
  std::cout << std::endl;

  // Update data in tables (see if it is reflected in the flows correctly).
  std::cout << "Update data in tables ... " << std::endl;
  for (std::string &update : UPDATES) {
    std::cout << std::endl;
    CHECK(pelton::exec(&connection, update).ok());
  }
  std::cout << std::endl;

  // Read flow.
  std::cout << "Read flows ... " << std::endl;
  for (const auto &query : FLOW_READS) {
    std::cout << std::endl;
    auto status = pelton::exec(&connection, query);
    CHECK(status.ok());
    Print(std::move(status.value()));
  }
  std::cout << std::endl;

  // Deletes.
  std::cout << "Delete data from tables ... " << std::endl;
  for (std::string &del : DELETES) {
    std::cout << std::endl;
    CHECK(pelton::exec(&connection, del).ok());
  }
  std::cout << std::endl;

  // Query data.
  std::cout << "Run queries ... " << std::endl;
  for (std::string &select : QUERIES) {
    std::cout << std::endl;
    CHECK(pelton::exec(&connection, select).ok());
  }
  std::cout << std::endl;

  // Read flow.
  std::cout << "Read flows ... " << std::endl;
  for (const auto &query : FLOW_READS) {
    std::cout << std::endl;
    auto status = pelton::exec(&connection, query);
    CHECK(status.ok());
    Print(std::move(status.value()));
  }
  std::cout << std::endl;

  // Print statistics.
  Print(connection.state->NumShards());
  std::cout << std::endl;
  Print(connection.state->SizeInMemory());
  std::cout << std::endl;

  // Close connection.
  pelton::close(&connection);
  pelton::shutdown();

  // Print performance profile.
  pelton::perf::PrintAll();

  // Done.
  std::cout << "exit" << std::endl;
  return 0;
}
