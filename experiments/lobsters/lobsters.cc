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
std::vector<std::string> CREATES;

// Printing query results.
void PrintHeader(const pelton::SqlResult &result) {
  for (size_t i = 0; i < result.getColumnCount(); i++) {
    std::cout << "| " << result.getColumn(i).getColumnName() << " ";
  }
  std::cout << "|" << std::endl;
  for (size_t i = 0; i < result.getColumnCount() * 10; i++) {
    std::cout << "-";
  }
  std::cout << std::endl;
}

void PrintRow(size_t column_count, const pelton::Row &row) {
  for (size_t i = 0; i < column_count; i++) {
    std::cout << "| ";
    row[i].print(std::cout);
    std::cout << " ";
  }
  std::cout << "|" << std::endl;
}

void Print(pelton::SqlResult &&result) {
  if (result.hasData()) {
    PrintHeader(result);
    while (result.count() > 0) {
      PrintRow(result.getColumnCount(), result.fetchOne());
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

  pelton::perf::Start("all");

  // Open connection to sharder.
  pelton::Connection connection;
  pelton::open("", db_username, db_password, &connection);
  CHECK(pelton::exec(&connection, "SET echo;").ok());

  // Create all the tables.
  std::cout << "Create the tables ... " << std::endl;
  for (std::string &create : CREATES) {
    std::cout << std::endl;
    CHECK(pelton::exec(&connection, create).ok());
  }
  std::cout << std::endl;

  // Add flows.
//  std::cout << "Installing flows ... " << std::endl;
//  for (const auto &[name, query] : FLOWS) {
//    std::cout << name << std::endl;
//    CHECK(pelton::exec(&connection, query).ok());
//  }
//  pelton::shutdown_planner();
//  std::cout << std::endl;
//
//  // Insert some data into the tables.
//  std::cout << "Insert data into tables ... " << std::endl;
//  for (std::string &insert : INSERTS) {
//    std::cout << std::endl;
//    CHECK(pelton::exec(&connection, insert).ok());
//  }
//  std::cout << std::endl;
//
//  // Read flow.
//  std::cout << "Read flows ... " << std::endl;
//  for (const auto &query : FLOW_READS) {
//    std::cout << std::endl;
//    auto status = pelton::exec(&connection, query);
//    CHECK(status.ok());
//    Print(std::move(status.value()));
//  }
//  std::cout << std::endl;
//
//  // Update data in tables (see if it is reflected in the flows correctly).
//  std::cout << "Update data in tables ... " << std::endl;
//  for (std::string &update : UPDATES) {
//    std::cout << std::endl;
//    CHECK(pelton::exec(&connection, update).ok());
//  }
//  std::cout << std::endl;
//
//  // Read flow.
//  std::cout << "Read flows ... " << std::endl;
//  for (const auto &query : FLOW_READS) {
//    std::cout << std::endl;
//    auto status = pelton::exec(&connection, query);
//    CHECK(status.ok());
//    Print(std::move(status.value()));
//  }
//  std::cout << std::endl;
//
//  // Deletes.
//  std::cout << "Delete data from tables ... " << std::endl;
//  for (std::string &del : DELETES) {
//    std::cout << std::endl;
//    CHECK(pelton::exec(&connection, del).ok());
//  }
//  std::cout << std::endl;
//
//  // Query data.
//  std::cout << "Run queries ... " << std::endl;
//  for (std::string &select : QUERIES) {
//    std::cout << std::endl;
//    CHECK(pelton::exec(&connection, select).ok());
//  }
//  std::cout << std::endl;
//
//  // Read flow.
//  std::cout << "Read flows ... " << std::endl;
//  for (const auto &query : FLOW_READS) {
//    std::cout << std::endl;
//    auto status = pelton::exec(&connection, query);
//    CHECK(status.ok());
//    Print(std::move(status.value()));
//  }
//  std::cout << std::endl;
//
//  // Close connection.
//  pelton::close(&connection);
//
//  // Print performance profile.
//  pelton::perf::End("all");
//  pelton::perf::PrintAll();
//
  // Done.
  std::cout << "exit" << std::endl;
  return 0;
}
