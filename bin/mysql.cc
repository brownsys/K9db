#include <sqlite3.h>

#include <iostream>
#include <limits>
#include <string>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mysql-cppconn-8/mysqlx/xdevapi.h"
#include "pelton/util/perf.h"

void PrintHeader(mysqlx::SqlResult *result) {
  for (size_t i = 0; i < result->getColumnCount(); i++) {
    std::cout << "| " << result->getColumn(i).getColumnName() << " ";
  }
  std::cout << "|" << std::endl;
  for (size_t i = 0; i < result->getColumnCount() * 10; i++) {
    std::cout << "-";
  }
  std::cout << std::endl;
}

void PrintRow(size_t column_count, mysqlx::Row *row) {
  for (size_t i = 0; i < column_count; i++) {
    std::cout << "| ";
    row->get(i).print(std::cout);
    std::cout << " ";
  }
  std::cout << "|" << std::endl;
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

  pelton::perf::Start("All");

  // Initialize our sharded state/connection.
  try {
    mysqlx::Session session(db_username + ":" + db_password + "@localhost");
    session.sql("CREATE DATABASE IF NOT EXISTS gdprbench").execute();
    session.sql("USE gdprbench").execute();

    std::cout << "Vanilla MySql" << std::endl;
    if (print) {
      std::cout << ">>> " << std::flush;
    }

    // Read SQL statements one at a time!
    std::string command;
    while (ReadCommand(&command)) {
      pelton::perf::Start("exec");
      mysqlx::SqlResult result = session.sql(command).execute();
      if (result.hasData()) {
        if (print) {
          PrintHeader(&result);
        }
        while (result.count() > 0) {
          mysqlx::Row row = result.fetchOne();
          if (print) {
            PrintRow(result.getColumnCount(), &row);
          }
        }
      }
      pelton::perf::End("exec");

      // Ready for next command.
      if (print) {
        std::cout << std::endl << ">>> " << std::flush;
      }
    }

    // Close the connection
    session.close();
  } catch (const char *err_msg) {
    LOG(FATAL) << "Error: " << err_msg;
  }

  pelton::perf::End("All");
  pelton::perf::PrintAll();

  // Exit!
  std::cout << "exit" << std::endl;

  return 0;
}
