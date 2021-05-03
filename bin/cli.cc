// NOLINTNEXTLINE
#include <chrono>
#include <iostream>
#include <limits>
#include <string>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "pelton/pelton.h"
#include "pelton/util/perf.h"

// Printing query results.
void Print(pelton::SqlResult &&result) {
  if (result.IsStatement()) {
    std::cout << "Success: " << result.Success() << std::endl;
  } else if (result.IsUpdate()) {
    std::cout << "Affected rows: " << result.UpdateCount() << std::endl;
  } else if (result.IsQuery()) {
    std::cout << result.GetSchema() << std::endl;
    for (const pelton::Record &record : result) {
      std::cout << record << std::endl;
    }
  }
}

// Read SQL commands one at a time.
bool ReadCommand(std::string *ptr) {
  pelton::perf::Start("Read std::cin");
  ptr->clear();
  std::string line;
  while (std::getline(std::cin, line)) {
    if (line.size() == 0 ||
        line.find_first_not_of(" \t\n") == std::string::npos) {
      continue;
    }
    if (line.front() == '#') {
      *ptr = line;
      pelton::perf::End("Read std::cin");
      return true;
    }
    // Wait until command is fully read (in case it spans several lines).
    *ptr += line;
    if (line.back() == '\\') {
      ptr->pop_back();
      continue;
    }

    // Done, read command successfully.
    pelton::perf::End("Read std::cin");
    return true;
  }

  pelton::perf::End("Read std::cin");
  return false;
}

DEFINE_bool(print, true, "Print results to the screen");
DEFINE_string(db_path, "", "Path to database directory (required)");
DEFINE_string(db_username, "root", "MYSQL username to connect with");
DEFINE_string(db_password, "password", "MYSQL pwd to connect with");

int main(int argc, char **argv) {
  // Command line arugments and help message.
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // don't skip the whitespace while reading
  std::cin >> std::noskipws;

  // Should we print outputs to the screen?
  bool print = FLAGS_print;

  // Initialize Google’s logging library.
  google::InitGoogleLogging("argc");

  // Find database directory.
  const std::string &db_username = FLAGS_db_username;
  const std::string &db_password = FLAGS_db_password;
  const std::string &dir = FLAGS_db_path;

  // Initialize our sharded state/connection.
  std::chrono::time_point<std::chrono::high_resolution_clock> start_time;
  std::chrono::time_point<std::chrono::high_resolution_clock> end_time;
  try {
    pelton::Connection connection;
    pelton::open(dir, db_username, db_password, &connection);

    std::cout << "SQL Sharder" << std::endl;
    std::cout << "DB directory: " << dir << std::endl;
    if (print) {
      std::cout << ">>> " << std::flush;
    }

    // Read SQL statements one at a time!
    std::string command;
    while (ReadCommand(&command)) {
      if (command[0] == '#' || (command[0] == '-' && command[1] == '-')) {
        if (command == "# perf start") {
          std::cout << "Perf start" << std::endl;
          pelton::perf::Start();
          start_time = std::chrono::high_resolution_clock::now();
        }
        continue;
      }

      // Command has been fully read, execute it!
      pelton::perf::Start("exec");
      absl::StatusOr<pelton::SqlResult> status =
          pelton::exec(&connection, command);
      if (!status.ok()) {
        std::cerr << "Fatal error" << std::endl;
        std::cerr << status.status() << std::endl;
        break;
      }

      // Print result.
      if (print) {
        Print(std::move(status.value()));
        std::cout << std::endl << ">>> " << std::flush;
      }
      pelton::perf::End("exec");
    }

    // Close the connection
    end_time = std::chrono::high_resolution_clock::now();
    pelton::close(&connection);
  } catch (const char *err_msg) {
    LOG(FATAL) << "Error: " << err_msg;
  }

  // Print performance profile.
  pelton::perf::PrintAll();
  std::cout << "Time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                                     start_time)
                   .count()
            << "ms" << std::endl;

  // Exit!
  std::cout << "exit" << std::endl;

  return 0;
}
