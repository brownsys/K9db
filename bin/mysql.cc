#include <sqlite3.h>

#include <iostream>
#include <limits>
#include <string>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mysql-cppconn-8/jdbc/cppconn/datatype.h"
#include "mysql-cppconn-8/jdbc/cppconn/resultset.h"
#include "mysql-cppconn-8/jdbc/cppconn/resultset_metadata.h"
#include "mysql-cppconn-8/jdbc/cppconn/sqlstring.h"
#include "mysql-cppconn-8/jdbc/cppconn/statement.h"
#include "mysql-cppconn-8/jdbc/mysql_connection.h"
#include "mysql-cppconn-8/jdbc/mysql_driver.h"
#include "pelton/util/perf.h"

void PrintHeader(sql::ResultSet *result) {
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

void PrintData(sql::ResultSet *result) {
  while (result->next()) {
    for (size_t i = 1; i <= result->getMetaData()->getColumnCount(); i++) {
      switch (result->getMetaData()->getColumnType(i)) {
        case sql::DataType::VARCHAR:
        case sql::DataType::CHAR:
        case sql::DataType::LONGVARCHAR:
          std::cout << "| " << result->getString(i) << " ";
          break;
        case sql::DataType::TINYINT:
        case sql::DataType::SMALLINT:
        case sql::DataType::MEDIUMINT:
        case sql::DataType::INTEGER:
          std::cout << "| " << result->getInt(i) << " ";
          break;
        default:
          std::cout << std::endl;
          std::cout << "Unknown column type: "
                    << result->getMetaData()->getColumnTypeName(i) << std::endl;
      }
    }
    std::cout << std::endl;
  }
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
    sql::Driver *driver = sql::mysql::get_driver_instance();
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

    // Read SQL statements one at a time!
    std::string command;
    while (ReadCommand(&command)) {
      if (command[0] == '#') {
        continue;
      }

      if (print) {
        std::cout << command << std::endl;
      }

      pelton::perf::Start("exec");
      if (command[0] == 'S' || command[0] == 's') {
        std::unique_ptr<sql::ResultSet> result{stmt->executeQuery(command)};
        if (print) {
          PrintHeader(result.get());
          PrintData(result.get());
        }
      } else if (command[0] == 'I' || command[0] == 'i' || command[0] == 'U' ||
                 command[0] == 'u' || command[0] == 'D' || command[0] == 'd') {
        if (print) {
          std::cout << stmt->executeUpdate(command) << " updated." << std::endl;
        }
      } else if (command[0] == 'C' || command[0] == 'c') {
        stmt->execute(command);
      }

      pelton::perf::End("exec");

      // Ready for next command.
      if (print) {
        std::cout << std::endl << ">>> " << std::flush;
      }
    }
  } catch (const char *err_msg) {
    LOG(FATAL) << "Error: " << err_msg;
  }

  pelton::perf::End("All");
  pelton::perf::PrintAll();

  // Exit!
  std::cout << "exit" << std::endl;

  return 0;
}
