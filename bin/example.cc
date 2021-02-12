#include <iostream>
#include <utility>

#include "pelton/pelton.h"

// CREATE TABLE queries.
std::vector<std::string> CREATES{
    // Students.
    "CREATE TABLE students ("
    "ID int,"
    "PII_Name varchar(100),"
    "PRIMARY KEY(ID)"
    ");",
    // Assignments.
    "CREATE TABLE assignments ("
    "ID int,"
    "Name varchar(100),"
    "PRIMARY KEY(ID)"
    ");",
    // Submissions.
    "CREATE TABLE submissions ("
    "ID int,"
    "student_id int,"
    "assignment_id int,"
    "timestamp int,"
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
    "INSERT INTO submissions VALUES (7, 3, 2, 7);"};

// Flows.
std::vector<std::pair<std::string, std::string>> FLOWS{std::make_pair(
    "FILTER FLOW", "SELECT * FROM submissions WHERE timestamp >= 100;")};

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

int main(int argc, char **argv) {
  // Read command line arguments.
  if (argc < 2) {
    std::cout << "Please provide the database directory as a command line "
                 "argument!"
              << std::endl;
    return 1;
  }
  std::string dir(argv[1]);

  // Open connection to sharder.
  pelton::shards::SharderState state;
  pelton::open(dir, &state);
  pelton::exec(&state, "SET echo;", &Callback, nullptr, nullptr);

  // Create all the tables.
  std::cout << "Create the tables ... " << std::endl;
  for (std::string &create : CREATES) {
    std::cout << std::endl;
    pelton::exec(&state, create, &Callback, nullptr, nullptr);
  }
  std::cout << std::endl;

  // Add flows.
  std::cout << "Installing flows ... " << std::endl;
  for (const auto &[name, query] : FLOWS) {
    std::cout << std::endl;
    pelton::make_view(&state, name, query);
  }
  std::cout << std::endl;

  // Insert some data into the tables.
  std::cout << "Insert data into tables ... " << std::endl;
  for (std::string &insert : INSERTS) {
    std::cout << std::endl;
    pelton::exec(&state, insert, &Callback, nullptr, nullptr);
  }
  std::cout << std::endl;

  // Query data.
  std::cout << "Run queries ... " << std::endl;
  for (std::string &select : QUERIES) {
    std::cout << std::endl;
    bool context = true;
    pelton::exec(&state, select, &Callback, reinterpret_cast<void *>(&context),
                 nullptr);
  }
  std::cout << std::endl;

  // Read flow.
  std::cout << "Read flows ... " << std::endl;
  for (const auto &[name, _] : FLOWS) {
    std::cout << std::endl;
    pelton::print_view(&state, name);
  }
  std::cout << std::endl;

  // Close connection.
  pelton::close(&state);
  std::cout << "exit" << std::endl;
  return 0;
}
