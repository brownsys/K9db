#include <iostream>
#include <string>

#include "pelton/pelton.h"

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

  // Initialize our sharded state/connection.
  pelton::Connection connection;
  pelton::open(dir, &connection);

  std::cout << "SQL Sharder" << std::endl;
  std::cout << "DB directory: " << dir << std::endl;
  std::cout << ">>> " << std::flush;

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
    try {
      bool context = true;
      pelton::exec(&connection, command, &Callback,
                   reinterpret_cast<void *>(&context), nullptr);
    } catch (const char *err_msg) {
      std::cerr << "Error: " << err_msg << std::endl;
      return 1;
    }

    // Ready for next command.
    std::cout << std::endl << ">>> " << std::flush;
    command = "";
  }

  // Close the connection
  pelton::close(&connection);

  // Exit!
  std::cout << "exit" << std::endl;
  return 0;
}
