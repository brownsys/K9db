// NOLINTNEXTLINE
#include <chrono>
// NOLINTNEXTLINE
#include <unistd.h>
// NOLINTNEXTLINE
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "pelton/pelton.h"

namespace {

// Read an SQL file one command at a time.
std::vector<std::string> ReadSQL(const std::string &file) {
  std::ifstream stream(file);
  if (!stream.is_open()) {
    LOG(FATAL) << "Could not open file '" << file << "'";
  }

  // Read the file one command at a time.
  // A command ends with a non-escaped new line character.
  std::string line;
  std::string command = "";
  std::vector<std::string> content;
  while (std::getline(stream, line)) {
    // if line is empty or commented out, skip it
    if (line == "" || (line[0] == '-' && line[1] == '-') || line[0] == '#') {
      continue;
    }

    // append line to command until reaching a non-escaped empty line.
    if (line[line.size() - 1] != '\\') {
      command.append(line);
      content.push_back(std::move(command));
      command = "";
    } else {
      line.pop_back();
      command.append(line);
    }
  }
  stream.close();
  return content;
}

// Drop the database (if it exists).
void DropDatabase(const std::string &db_name) {
  std::filesystem::remove_all("/tmp/pelton/rocksdb/" + db_name);
}

double freq(size_t count, uint64_t time) {
  return static_cast<double>(count) / (static_cast<double>(time) / 1000 / 1000);
}

}  // namespace

#define DATABASE "cli"
#define PRIME "bin/bench/prime.sql"
#define RUN "bin/bench/run.sql"

int main(int argc, char **argv) {
  DropDatabase(DATABASE);

  // Initialize pelton connection.
  pelton::Connection connection;
  pelton::initialize(1, true);
  pelton::open(&connection, DATABASE);
  std::cout << "Pelton Initialized" << std::endl;
  std::cout << std::endl;

  // PRIME database.
  auto commands = ReadSQL(PRIME);
  auto s = std::chrono::steady_clock::now();
  for (const std::string &cmd : commands) {
    auto status = pelton::exec(&connection, cmd);
    if (!status.ok()) {
      std::cout << status.status() << std::endl;
      return -1;
    }
  }
  auto e = std::chrono::steady_clock::now();
  auto d = std::chrono::duration_cast<std::chrono::microseconds>(e - s).count();
  std::cout << "Priming complete " << d << "[us]" << std::endl;
  std::cout << "Priming " << freq(commands.size(), d) << "ops/sec" << std::endl;
  std::cout << std::endl;

  // RUN load.
  commands = ReadSQL(RUN);
  sleep(5);
  s = std::chrono::steady_clock::now();
  for (size_t i = 0; i < 50; i++) {
    for (const std::string &cmd : commands) {
      auto status = pelton::exec(&connection, cmd);
      if (!status.ok()) {
        std::cout << status.status() << std::endl;
        return -1;
      } else {
        pelton::SqlResult result = std::move(status.value());
        if (result.IsQuery()) {
          for (pelton::SqlResultSet &resultset : result.ResultSets()) {
            std::vector<pelton::Record> records = resultset.Vec();
          }
        }
      }
    }
  }

  e = std::chrono::steady_clock::now();
  d = std::chrono::duration_cast<std::chrono::microseconds>(e - s).count();
  std::cout << "Run complete " << d << "[us]" << std::endl;
  std::cout << "Run " << freq(commands.size(), d) << "ops/sec" << std::endl;
  std::cout << std::endl;

  // Shutdown.
  pelton::close(&connection);
  pelton::shutdown();
}
