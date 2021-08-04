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
Connection *pelton_state = nullptr;

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

bool SpecialStatements(const std::string &sql, Connection *connection) {
  if (sql == "SET echo;" || sql == "SET echo") {
    echo = true;
    std::cout << "SET echo;" << std::endl;
    return true;
  }
  return false;
}

}  // namespace

bool global_open(const std::string &directory, const std::string &db_name,
                 const std::string &db_username,
                 const std::string &db_password) {
  // if already open
  if (pelton_state != nullptr) {
    // close without shutting down planner
    global_close(false);
  }
  pelton_state = new Connection();
  pelton_state->Initialize(directory);
  pelton_state->GetSharderState()->Initialize(db_name, db_username,
                                              db_password);
  pelton_state->Load();
  return true;
}

bool open(ConnectionLocal *connection) {
  connection->global_connection = pelton_state;
  return true;
}

bool close(ConnectionLocal *connection) {
  // empty for now
  return true;
}

absl::StatusOr<SqlResult> exec(ConnectionLocal *connection, std::string sql) {
  // Trim statement.
  Trim(sql);
  if (echo) {
    std::cout << sql << std::endl;
  }

  // If special statement, handle it separately.
  if (SpecialStatements(sql, connection->global_connection)) {
    return SqlResult(true);
  }

  // Parse and rewrite statement.
  shards::SharderState *sstate =
      connection->global_connection->GetSharderState();  // access field?
  dataflow::DataFlowState *dstate =
      connection->global_connection->GetDataFlowState();
  return shards::sqlengine::Shard(sql, sstate, dstate);
}

void shutdown_planner() { planner::ShutdownPlanner(); }

bool global_close(bool shutdown_planner) {
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
