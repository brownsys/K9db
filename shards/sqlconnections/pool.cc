// Manages sqlite3 connections to the different shard/mini-databases.

#include "shards/sqlconnections/pool.h"

#include <cstring>
#include <vector>

namespace shards {
namespace sqlconnections {

static std::vector<char *> column_names;
static std::vector<std::vector<char *>> data;
static std::function<void(int *, char ***, char ***)> current_modifier;

static void DeleteNames() {
  for (char *ptr : column_names) {
    delete[] ptr;
  }
  column_names.clear();
}

static void DeleteData() {
  for (auto &nested : data) {
    for (char *ptr : nested) {
      delete[] ptr;
    }
  }
  data.clear();
}

static bool Duplicated(char **c) {
  for (auto &nested : data) {
    bool all_eq = true;
    for (size_t i = 0; i < nested.size(); i++) {
      if (strcmp(nested.at(i), c[i]) != 0) {
        all_eq = false;
        break;
      } else {
      }
    }
    if (all_eq) {
      return true;
    }
  }
  return false;
}

static int BufferingCallback(void *_, int b, char **c, char **d) {
  // Overwrite names.
  DeleteNames();
  column_names.clear();
  // Copy and deduplicate.
  current_modifier(&b, &c, &d);
  bool dup = Duplicated(c);
  // Insert into buffer.
  if (!dup) {
    data.push_back(std::vector<char *>());
  }
  for (int i = 0; i < b; i++) {
    if (!dup) {
      data.at(data.size() - 1).push_back(c[i]);
    }
    column_names.push_back(d[i]);
  }
  return 0;
}

// Destructor
ConnectionPool::~ConnectionPool() { ::sqlite3_close(this->main_connection_); }

// Initialization.
void ConnectionPool::Initialize(const std::string &dir_path) {
  this->dir_path_ = dir_path;
  std::string shard_path = dir_path + std::string("default.sqlite3");
  ::sqlite3_open(shard_path.c_str(), &this->main_connection_);
}

bool ConnectionPool::ExecuteStatement(
    const std::string &shard_suffix, const std::string &statement,
    std::function<void(int *, char ***, char ***)> modifier, char **errmsg) {
  // Open connection (if needed).
  sqlite3 *connection = this->main_connection_;
  if (shard_suffix != "default") {
    std::string shard_path =
        this->dir_path_ + shard_suffix + std::string(".sqlite3");
    ::sqlite3_open(shard_path.c_str(), &connection);
  }

  // Execute query.
  current_modifier = modifier;
  int result_code = ::sqlite3_exec(connection, statement.c_str(),
                                   BufferingCallback, nullptr, errmsg);

  // Handle errors.
  if (result_code != SQLITE_OK) {
    throw "SQLITE3 exeuction error!";
  }
  // Close connection (if needed).
  if (shard_suffix != "default") {
    ::sqlite3_close(connection);
  }

  return result_code == SQLITE_OK;
}

void ConnectionPool::FlushBuffer(
    std::function<int(void *, int, char **, char **)> cb, void *context,
    char **errmsg) {
  // Call the callback after buffering and de-duplicating.
  for (auto &nested : data) {
    if (cb(context, nested.size(), &nested.at(0), &column_names.at(0)) != 0) {
      break;
    }
  }

  // Free buffers.
  DeleteData();
  DeleteNames();
}

}  // namespace sqlconnections
}  // namespace shards
