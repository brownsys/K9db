// Defines the API for our SQLite-adapter.
#include "pelton/pelton.h"

#include <iostream>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "pelton/planner/planner.h"
#include "pelton/shards/sqlengine/engine.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/status.h"

namespace pelton {

namespace {

// global state that persists between client connections
State *pelton_state = nullptr;

// String whitespace trimming.
// https://stackoverflow.com/questions/216823/whats-the-best-way-to-trim-stdstring/25385766
// NOLINTNEXTLINE
static inline void Trim(std::string &s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
            return !std::isspace(ch);
          }));
  s.erase(std::find_if(s.rbegin(), s.rend(),
                       [](unsigned char ch) { return !std::isspace(ch); })
              .base(),
          s.end());
}

// Special statements we added to SQL.
bool echo = false;

bool SpecialStatements(const std::string &sql, State *connection) {
  if (sql == "SET echo;" || sql == "SET echo") {
    echo = true;
    std::cout << "SET echo;" << std::endl;
    return true;
  }
  return false;
}

}  // namespace

bool initialize(const std::string &directory) {
  // if already open
  if (pelton_state != nullptr) {
    // close without shutting down planner
    shutdown(false);
  }
  pelton_state = new State();
  // initialize path_ in State with &directory (path to db)
  pelton_state->Initialize(directory);
  pelton_state->Load();
  return true;
}

bool open(Connection *connection, const std::string &db_name,
          const std::string &db_username, const std::string &db_password) {
  // set global state in local connection struct
  connection->pelton_state = pelton_state;
  // run SqlEagerExecutor::Initialize, initializing mariadb connection
  connection->executor_.Initialize(db_name, db_username, db_password);
  return true;
}

bool close(Connection *connection) {
  // empty for now
  return true;
}

absl::StatusOr<SqlResult> exec(Connection *connection, std::string sql) {
  // Trim statement.
  Trim(sql);
  if (echo) {
    std::cout << sql << std::endl;
  }
  // If special statement, handle it separately.
  if (SpecialStatements(sql, connection->pelton_state)) {
    return SqlResult(true);
  }
  // Parse and rewrite statement, passing local connection with ptr to global
  // state containing sharder and dataflow state
  absl::lts_2020_09_23::StatusOr<pelton::sql::SqlResult> sql_result =
      shards::sqlengine::Shard(sql, connection);
  return sql_result;
}

void shutdown_planner() { planner::ShutdownPlanner(); }

bool shutdown(bool shutdown_planner) {
  if (pelton_state == nullptr) {
    return true;
  }
  pelton_state->Save();
  if (shutdown_planner) {
    planner::ShutdownPlanner();
  }
  delete pelton_state;
  pelton_state = nullptr;
  return true;
}

}  // namespace pelton
