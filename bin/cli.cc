// NOLINTNEXTLINE
#include <chrono>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "pelton/pelton.h"
#include "pelton/util/latency.h"

std::vector<std::string> TO_SKIP = {"submit"};

// Printing query results.
void Print(pelton::SqlResult &&result, bool print) {
  if (print) {
    if (result.IsStatement()) {
      std::cout << "Success: " << result.Success() << std::endl;
    } else if (result.IsUpdate()) {
      std::cout << "Affected rows: " << result.UpdateCount() << std::endl;
    } else if (result.IsQuery()) {
      for (pelton::SqlResultSet &resultset : result.ResultSets()) {
        std::cout << resultset.schema() << std::endl;
        std::vector<pelton::Record> records = resultset.Vec();
        for (pelton::Record &record : records) {
          std::cout << record << std::endl;
        }
      }
    }
  }
}

// Read SQL commands one at a time.
bool ReadCommand(std::string *ptr) {
  ptr->clear();
  std::string line;
  while (std::getline(std::cin, line)) {
    if (line.size() == 0 ||
        line.find_first_not_of(" \t\n") == std::string::npos) {
      continue;
    }
    if (line.front() == '#' || (line[0] == '-' && line[1] == '-')) {
      *ptr = line;
      return true;
    }
    // Wait until command is fully read (in case it spans several lines).
    *ptr += line;
    if (line.back() == '\\') {
      ptr->pop_back();
      continue;
    }

    // Done, read command successfully.
    return true;
  }

  return false;
}

DEFINE_bool(print, true, "Print results to the screen");
DEFINE_bool(progress, true, "Show progress counter");
DEFINE_int32(workers, 3, "Number of workers");
DEFINE_bool(consistent, true, "Dataflow consistency with futures");
DEFINE_string(db_name, "pelton", "Name of the database");

int main(int argc, char **argv) {
  // Command line arugments and help message.
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // don't skip the whitespace while reading
  std::cin >> std::noskipws;

  // Should we print outputs to the screen?
  bool print = FLAGS_print;

  // Initialize Google’s logging library.
  google::InitGoogleLogging("cli");

  // Valdiate command line flags.
  if (FLAGS_workers < 0) {
    std::cout << "Bad number of workers" << std::endl;
    return -1;
  }

  // Read command line flags.
  size_t workers = static_cast<size_t>(FLAGS_workers);
  bool consistent = FLAGS_consistent;
  const std::string &db_name = FLAGS_db_name;
  size_t progress = 0;

  // Initialize our sharded state/connection.
  std::chrono::time_point<std::chrono::high_resolution_clock> start_time;
  std::chrono::time_point<std::chrono::high_resolution_clock> end_time;
  try {
    pelton::initialize(workers, consistent);

    pelton::Connection connection;
    pelton::open(&connection, db_name);

    std::cout << "SQL Sharder" << std::endl;
    std::cout << "DB: " << db_name << std::endl;
    if (print) {
      std::cout << ">>> " << std::flush;
    }

    // For Measuring Latency of composie endpoints.
    pelton::latency::Latency profiler;

    // Read SQL statements one at a time!
    std::string command;
    std::string current_endpoint = "";
    while (ReadCommand(&command)) {
      if (command[0] == '#') {
        if (command == "# perf start") {
          std::cout << "Perf start" << std::endl;
          current_endpoint = profiler.TurnOn();
          start_time = std::chrono::high_resolution_clock::now();
        }
        continue;
      } else if (command[0] == '-' && command[1] == '-') {
        current_endpoint = profiler.Measure(command);
        continue;
      } else if (std::find(TO_SKIP.begin(), TO_SKIP.end(), current_endpoint) !=
                 TO_SKIP.end()) {
        continue;
      }

      if (FLAGS_progress) {
        if (++progress % 1000 == 0) {
          std::cout << progress << std::endl;
        }
      }

      // Command has been fully read, execute it
      absl::StatusOr<pelton::SqlResult> status =
          pelton::exec(&connection, command);
      if (!status.ok()) {
        std::cerr << "Fatal error" << std::endl;
        std::cerr << status.status() << std::endl;
        std::cerr << command << std::endl;
        break;
      }
      Print(std::move(status.value()), print);

      // Print result.
      if (print) {
        std::cout << std::endl << ">>> " << std::flush;
      }
    }

    profiler.PrintAll();

    // Close the connection
    end_time = std::chrono::high_resolution_clock::now();

    // Display statistics.
    Print(connection.state->NumShards(), print);
    Print(connection.state->SizeInMemory(), print);

    auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(
        end_time - start_time);
    std::cout << "Time PELTON: " << diff.count() << "ns" << std::endl;

    pelton::close(&connection);
    pelton::shutdown();
  } catch (const char *err_msg) {
    LOG(FATAL) << "Error: " << err_msg;
  }

  // Exit!
  std::cout << "exit" << std::endl;

  return 0;
}
