// Defines the API for our SQLite-adapter.
#include "pelton/pelton.h"

#include <iostream>
#include <optional>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "pelton/planner/planner.h"
#include "pelton/shards/sqlengine/engine.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/util/status.h"

namespace pelton {

namespace {

// global state that persists between client connections
static State *PELTON_STATE = nullptr;

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

std::optional<SqlResult> SpecialStatements(const std::string &sql,
                                           Connection *connection) {
  if (sql == "SET echo;" || sql == "SET echo") {
    echo = true;
    return SqlResult(true);
  }
  if (absl::StartsWith(sql, "SHOW ")) {
    std::vector<std::string> split = absl::StrSplit(sql, ' ');
    if (absl::StartsWith(split.at(1), "VIEW")) {
      std::string &flow_name = split.at(2);
      if (flow_name.back() == ';') {
        flow_name.pop_back();
      }
      return connection->state->FlowDebug(flow_name);
    }
    if (absl::StartsWith(split.at(1), "MEMORY")) {
      return connection->state->SizeInMemory();
    }
    if (absl::StartsWith(split.at(1), "SHARDS")) {
      return connection->state->NumShards();
    }
  }
  return {};
}

}  // namespace

bool initialize(size_t workers, bool consistent) {
  // if already open
  if (PELTON_STATE != nullptr) {
    // close without shutting down planner
    shutdown(false);
  }
  PELTON_STATE = new State(workers, consistent);
  return true;
}

bool open(Connection *connection, const std::string &db_name,
          const std::string &db_username, const std::string &db_password) {
  // set global state in local connection struct
  connection->state = PELTON_STATE;
  // run SqlEagerExecutor::Initialize, initializing mariadb connection
  connection->executor.Initialize(db_name, db_username, db_password);
  return true;
}

bool close(Connection *connection) {
  connection->executor.Close();
  return true;
}

absl::StatusOr<SqlResult> exec(Connection *connection, std::string sql) {
  // Trim statement, removing whitespace
  Trim(sql);
  // print sql statements if echo is set to true
  if (echo) {
    std::cout << sql << std::endl;
  }
  // If special statement, handle it separately.
  auto special = SpecialStatements(sql, connection);
  if (special) {
    return std::move(special.value());
  }

  // Parse and rewrite statement.
  try {
    return shards::sqlengine::Shard(sql, connection);
  } catch (::sql::SQLException &e) {
    std::stringstream msg;
    msg << "MySQL exception encountered ";
    msg << e.getMessage() << " (" << e.what() << ")";

    return absl::InternalError(msg.str());
  } catch (std::exception &e) {
    return absl::InternalError(e.what());
  }
}

void shutdown_planner() { planner::ShutdownPlanner(); }

bool shutdown(bool shutdown_planner) {
  if (PELTON_STATE == nullptr) {
    return true;
  }
  if (shutdown_planner) {
    planner::ShutdownPlanner();
  }
  PELTON_STATE->dataflow_state()->Shutdown();
  delete PELTON_STATE;
  PELTON_STATE = nullptr;
  return true;
}

}  // namespace pelton
