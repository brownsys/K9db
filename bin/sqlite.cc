#include <sqlite3.h>

#include <iostream>
#include <limits>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/flags/usage.h"
#include "glog/logging.h"
#include "pelton/util/perf.h"

// Print query results!
int PrintCb(void *context, int col_count, char **col_data, char **col_name) {
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

// No-op.
int NoCb(void *context, int col_count, char **col_data, char **col_name) {
  return 0;
}

bool ReadCommand(std::string *ptr) {
  pelton::perf::Start("Read std::cin");
  ptr->clear();
  std::string line;
  while (std::getline(std::cin, line)) {
    if (line.size() == 0 || line.front() == '#' || line.front() == '.' ||
        line.find_first_not_of(" \t\n") == std::string::npos) {
      continue;
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

ABSL_FLAG(bool, print, true, "Print results to the screen");
ABSL_FLAG(std::string, db_path, "", "Path to database file (required)");

int main(int argc, char **argv) {
  // Command line arugments and help message.
  absl::SetProgramUsageMessage(
      "usage: bazel run //bin:sqlite "
      "--db_path=/path/to/db "
      "--print=<yes/no>");
  absl::ParseCommandLine(argc, argv);

  // Should we print outputs to the screen?
  bool print = absl::GetFlag(FLAGS_print);
  auto callback = print ? &PrintCb : NoCb;

  // Initialize Google’s logging library.
  google::InitGoogleLogging(argv[0]);

  // Find database directory.
  const std::string &db_path = absl::GetFlag(FLAGS_db_path);
  if (db_path == "") {
    LOG(FATAL) << "Please provide the database dir as a command line argument!";
  }

  pelton::perf::Start("All");

  // Initialize our sharded state/connection.
  try {
    ::sqlite3 *connection;
    ::sqlite3_open(db_path.c_str(), &connection);

    std::cout << "Vanilla SQLITE" << std::endl;
    std::cout << "DB path: " << db_path << std::endl;
    if (print) {
      std::cout << ">>> " << std::flush;
    }

    // Read SQL statements one at a time!
    std::string command;
    while (ReadCommand(&command)) {
      // Command has been fully read, execute it!
      bool context = true;
      char *errmsg = nullptr;

      pelton::perf::Start("exec");
      if (sqlite3_exec(connection, command.c_str(), callback, &context,
                       &errmsg) != SQLITE_OK) {
        std::cout << "Fatal error" << std::endl;
        if (errmsg != nullptr) {
          std::cout << errmsg << std::endl;
          ::sqlite3_free(errmsg);
        }
        break;
      }
      pelton::perf::End("exec");

      // Ready for next command.
      if (print) {
        std::cout << std::endl << ">>> " << std::flush;
      }
    }

    // Close the connection
    ::sqlite3_close(connection);
  } catch (const char *err_msg) {
    LOG(FATAL) << "Error: " << err_msg;
  }

  pelton::perf::End("All");
  pelton::perf::PrintAll();

  // Exit!
  std::cout << "exit" << std::endl;

  return 0;
}
