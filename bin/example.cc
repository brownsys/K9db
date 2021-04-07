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
    ");"};

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
    std::make_pair(
        "union_flow",
        "CREATE VIEW union_flow AS "
        "'\"(SELECT * FROM submissions WHERE ts >= 100) UNION (SELECT "
        "* FROM submissions WHERE ts < 100)\"'"),
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
                   "'\"SELECT * from submissions ORDER BY ts LIMIT ?\"'")};

std::vector<std::string> FLOW_READS{
    "SELECT * FROM filter_row;",
    "SELECT * FROM filter_row2;",
    "SELECT * FROM filter_row3;",
    "SELECT * FROM union_flow;",
    "SELECT * FROM join_flow;",
    "SELECT * FROM limit_constant;",
    "SELECT * FROM limit_variable WHERE ts > 100 LIMIT 2 OFFSET 1;"};

// Selects.
std::vector<std::string> QUERIES{
    "SELECT * FROM submissions;",
    "SELECT * FROM submissions WHERE student_id = 1;",
    "SELECT * FROM submissions WHERE assignment_id = 2;"};

// Print query results!
int Callback(void *context, int col_count, char **col_data, char **col_name) {
  bool *first_time = reinterpret_cast<bool *>(context);

  // Print header the first time!
  if (*first_time) {
    std::cout << std::endl;
    *first_time = false;
    for (int i = 0; i < col_count; i++) {
      std::cout << "| " << col_name[i] << " ";
    }
    std::cout << "|" << std::endl;
    for (int i = 0; i < col_count * 10; i++) {
      std::cout << "-";
    }
    std::cout << std::endl;
  }

  // Print row data.
  for (int i = 0; i < col_count; i++) {
    std::cout << "| " << col_data[i] << " ";
  }
  std::cout << "|" << std::endl;

  return 0;
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
  std::cout << db_username << db_password << std::endl;

  pelton::perf::Start("all");

  // Open connection to sharder.
  pelton::Connection connection;
  pelton::open("", db_username, db_password, &connection);
  CHECK(pelton::exec(&connection, "SET echo;", &Callback, nullptr, nullptr));

  // Create all the tables.
  std::cout << "Create the tables ... " << std::endl;
  for (std::string &create : CREATES) {
    std::cout << std::endl;
    CHECK(pelton::exec(&connection, create, &Callback, nullptr, nullptr));
  }
  std::cout << std::endl;

  // Add flows.
  std::cout << "Installing flows ... " << std::endl;
  for (const auto &[name, query] : FLOWS) {
    std::cout << name << std::endl;
    CHECK(pelton::exec(&connection, query, &Callback, nullptr, nullptr));
  }
  pelton::shutdown_planner();
  std::cout << std::endl;

  // Insert some data into the tables.
  std::cout << "Insert data into tables ... " << std::endl;
  for (std::string &insert : INSERTS) {
    std::cout << std::endl;
    CHECK(pelton::exec(&connection, insert, &Callback, nullptr, nullptr));
  }
  std::cout << std::endl;

  // Read flow.
  std::cout << "Read flows ... " << std::endl;
  for (const auto &query : FLOW_READS) {
    std::cout << std::endl;
    bool context = true;
    CHECK(pelton::exec(&connection, query, &Callback,
                        reinterpret_cast<void *>(&context), nullptr));
  }
  std::cout << std::endl;

  // Update data in tables (see if it is reflected in the flows correctly).
  std::cout << "Update data in tables ... " << std::endl;
  for (std::string &update : UPDATES) {
    std::cout << std::endl;
    CHECK(pelton::exec(&connection, update, &Callback, nullptr, nullptr));
  }
  std::cout << std::endl;

  // Read flow.
  std::cout << "Read flows ... " << std::endl;
  for (const auto &query : FLOW_READS) {
    std::cout << std::endl;
    bool context = true;
    CHECK(pelton::exec(&connection, query, &Callback,
                        reinterpret_cast<void *>(&context), nullptr));
  }
  std::cout << std::endl;

  // Deletes.
  std::cout << "Delete data from tables ... " << std::endl;
  for (std::string &del : DELETES) {
    std::cout << std::endl;
    CHECK(pelton::exec(&connection, del, &Callback, nullptr, nullptr));
  }
  std::cout << std::endl;

  // Query data.
  std::cout << "Run queries ... " << std::endl;
  for (std::string &select : QUERIES) {
    std::cout << std::endl;
    bool context = true;
    CHECK(pelton::exec(&connection, select, &Callback,
                        reinterpret_cast<void *>(&context), nullptr));
  }
  std::cout << std::endl;

  // Read flow.
  std::cout << "Read flows ... " << std::endl;
  for (const auto &query : FLOW_READS) {
    std::cout << std::endl;
    bool context = true;
    CHECK(pelton::exec(&connection, query, &Callback,
                        reinterpret_cast<void *>(&context), nullptr));
  }
  std::cout << std::endl;

  // Close connection.
  pelton::close(&connection);

  // Print performance profile.
  pelton::perf::End("all");
  pelton::perf::PrintAll();

  // Done.
  std::cout << "exit" << std::endl;
  return 0;
}
