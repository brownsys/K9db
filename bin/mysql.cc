// NOLINTNEXTLINE
#include <chrono>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mariadb/conncpp.hpp"
#include "pelton/util/latency.h"
#include "pelton/util/perf.h"

std::vector<std::string> TO_SKIP = {"user",    "submit",   "recent",
                                    "comment", "comments", "frontpage",
                                    "recent",  "story",    "story_vote"};

void PrintHeader(bool print, sql::ResultSet *result) {
  if (print) {
    sql::ResultSetMetaData *metadata = result->getMetaData();
    for (size_t i = 1; i <= metadata->getColumnCount(); i++) {
      std::cout << "| " << metadata->getColumnName(i) << " ";
    }
    std::cout << "|" << std::endl;
    for (size_t i = 0; i < metadata->getColumnCount() * 10; i++) {
      std::cout << "-";
    }
    std::cout << std::endl;
  }
}

void PrintData(bool print, sql::ResultSet *result) {
  std::unique_ptr<sql::ResultSetMetaData> meta{result->getMetaData()};
  while (result->next()) {
    for (size_t i = 1; i <= meta->getColumnCount(); i++) {
      switch (meta->getColumnType(i)) {
        case sql::DataType::VARCHAR:
        case sql::DataType::NVARCHAR:
        case sql::DataType::CHAR:
        case sql::DataType::NCHAR:
        case sql::DataType::LONGVARCHAR:
        case sql::DataType::LONGNVARCHAR:
        case sql::DataType::TIMESTAMP:  // Print date as string.
          if (print) {
            std::cout << "| " << result->getString(i) << " ";
          }
          break;
        case sql::DataType::TINYINT:
        case sql::DataType::SMALLINT:
        case sql::DataType::BIGINT:
        case sql::DataType::BIT:
        case sql::DataType::INTEGER:
          if (print) {
            std::cout << "| " << result->getInt(i) << " ";
          }
          break;
        default:
          std::cout << std::endl;
          std::cout << "Unknown column type: " << meta->getColumnTypeName(i)
                    << " #" << meta->getColumnType(i) << std::endl;
      }
    }
    if (print) {
      std::cout << std::endl;
    }
  }
}

bool ReadCommand(std::string *ptr) {
  pelton::perf::Start("Read std::cin");
  ptr->clear();
  std::string line;
  while (std::getline(std::cin, line)) {
    if (line.size() == 0 || line.front() == '.' ||
        line.find_first_not_of(" \t\n") == std::string::npos) {
      continue;
    }
    if (line.front() == '#' || (line[0] == '-' && line[1] == '-')) {
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

  // Initialize our sharded state/connection.
  std::chrono::time_point<std::chrono::high_resolution_clock> start_time;
  std::chrono::time_point<std::chrono::high_resolution_clock> end_time;
  try {
    sql::Driver *driver = sql::mariadb::get_driver_instance();
    std::unique_ptr<sql::Connection> con{
        driver->connect("localhost", db_username, db_password)};

    // We execute things via this statement.
    std::unique_ptr<sql::Statement> stmt{con->createStatement()};
    stmt->execute("CREATE DATABASE IF NOT EXISTS gdprbench");
    stmt->execute("USE gdprbench");

    std::cout << "Vanilla MySql" << std::endl;
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
          pelton::perf::Start();
          current_endpoint = profiler.TurnOn();
          start_time = std::chrono::high_resolution_clock::now();
        }
        continue;
      } else if (command[0] == '-' && command[1] == '-') {
        current_endpoint = profiler.Measure(command);
        continue;
      } else if (command.substr(0, 8) == "REPLACE ") {
        continue;
      } else if (std::find(TO_SKIP.begin(), TO_SKIP.end(), current_endpoint) !=
                 TO_SKIP.end()) {
        continue;
      }

      if (print) {
        std::cout << command << std::endl;
      }

      pelton::perf::Start("exec");
      if (command[0] == 'S' || command[0] == 's') {
        std::unique_ptr<sql::ResultSet> result{stmt->executeQuery(command)};
        PrintHeader(print, result.get());
        PrintData(print, result.get());
      } else if (command[0] == 'I' || command[0] == 'i' || command[0] == 'U' ||
                 command[0] == 'u' || command[0] == 'D' || command[0] == 'd') {
        int count = stmt->executeUpdate(command);
        if (print) {
          std::cout << count << " updated." << std::endl;
        }
      } else if (command[0] == 'C' || command[0] == 'c') {
        stmt->execute(command);
      }

      pelton::perf::End("exec");
      profiler.PrintAll();

      // Ready for next command.
      if (print) {
        std::cout << std::endl << ">>> " << std::flush;
      }
    }

    end_time = std::chrono::high_resolution_clock::now();
  } catch (const char *err_msg) {
    LOG(FATAL) << "Error: " << err_msg;
  }

  pelton::perf::PrintAll();
  std::cout << "Time MYSQL: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                                     start_time)
                   .count()
            << "ms" << std::endl;

  // Exit!
  std::cout << "exit" << std::endl;

  return 0;
}
