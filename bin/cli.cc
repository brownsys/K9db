#include <iostream>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/flags/usage.h"
#include "glog/logging.h"
#include "pelton/pelton.h"

ABSL_FLAG(bool, print, true, "Print results to the screen");
ABSL_FLAG(std::string, db_path, "", "Path to database directory (required)");

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

int main(int argc, char **argv) {
  // Command line arugments and help message.
  absl::SetProgramUsageMessage(
      "usage: bazel run //bin:cli "
      "--db_path=/path/to/db/directory "
      "--print=<yes/no>");
  absl::ParseCommandLine(argc, argv);

  // Should we print outputs to the screen?
  bool print = absl::GetFlag(FLAGS_print);
  auto callback = print ? &PrintCb : NoCb;

  // Initialize Google’s logging library.
  google::InitGoogleLogging(argv[0]);

  // Find database directory.
  const std::string &dir = absl::GetFlag(FLAGS_db_path);
  if (dir == "") {
    LOG(FATAL) << "Please provide the database dir as a command line argument!";
  }

  // Initialize our sharded state/connection.
  try {
    pelton::Connection connection;
    pelton::open(dir, &connection);

    std::cout << "SQL Sharder" << std::endl;
    std::cout << "DB directory: " << dir << std::endl;
    if (print) {
      std::cout << ">>> " << std::flush;
    }

    // Read SQL statements one at a time!
    std::string line;
    std::string command = "";
    while (std::getline(std::cin, line)) {
      if (line.size() == 0 || line.front() == '#') {
        continue;
      }
      // Wait until command is fully read (in case it spans several lines).
      command += line;
      if (command.size() > 0 && command.back() == '\\') {
        command.pop_back();
        continue;
      }
      if (command.find_first_not_of(" \t\n") == std::string::npos) {
        // all white space
        continue;
      }

      // Command has been fully read, execute it!
      bool context = true;
      char *errmsg = nullptr;
      if (!pelton::exec(&connection, command, callback,
                        reinterpret_cast<void *>(&context), &errmsg)) {
        std::cerr << "Fatal error" << std::endl;
        if (errmsg != nullptr) {
          std::cerr << errmsg << std::endl;
          ::sqlite3_free(errmsg);
        }
        break;
      }

      // Ready for next command.
      if (print) {
        std::cout << std::endl << ">>> " << std::flush;
      }
      command = "";
    }

    // Close the connection
    pelton::close(&connection);
  } catch (const char *err_msg) {
    LOG(FATAL) << "Error: " << err_msg;
  }

  // Exit!
  std::cout << "exit" << std::endl;

  return 0;
}
