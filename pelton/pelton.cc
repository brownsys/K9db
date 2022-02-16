// Defines the API for our SQLite-adapter.
#include "pelton/pelton.h"

#include <iostream>
#include <optional>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "pelton/dataflow/graph.h"
#include "pelton/planner/planner.h"
#include "pelton/shards/sqlengine/engine.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/shards/upgradable_lock.h"
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
    return false;
  }
  PELTON_STATE = new State(workers, consistent);
  return true;
}

bool open(Connection *connection, const std::string &db_name) {
  // set global state in local connection struct
  connection->state = PELTON_STATE;
  connection->executor.Initialize(db_name);
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
  } catch (std::exception &e) {
    return absl::InternalError(e.what());
  }
}

void shutdown_planner(bool shutdown_jvm) {
  planner::ShutdownPlanner(shutdown_jvm);
}

bool shutdown(bool shutdown_jvm) {
  if (PELTON_STATE != nullptr) {
    shutdown_planner(shutdown_jvm);
    delete PELTON_STATE;
    PELTON_STATE = nullptr;
    return true;
  }
  return false;
}

// Prepared Statement API.
absl::StatusOr<const PreparedStatement *> prepare(Connection *connection,
                                                  const std::string &query) {
  // Acquire a reader shared lock.
  shards::SharedLock reader_lock = connection->state->ReaderLock();

  // Canonicalize the query.
  prepared::CanonicalQuery canonical = prepared::Canonicalize(query);

  // Extract information about the count of each parameter.
  prepared::PreparedStatementDescriptor stmt = prepared::MakeStmt(query);

  // Check if canonicalized query was handled before (avoids duplicating views).
  auto &stmts = connection->state->stmts();
  if (stmts.find(canonical) == stmts.end()) {
    // Upgrade lock to a writer lock, we need to modify stmts.
    shards::UniqueLock upgraded(std::move(reader_lock));
    // Make sure another thread did not create the canonical statement while we
    // were upgrading.
    if (stmts.find(canonical) == stmts.end()) {
      prepared::CanonicalDescriptor descriptor =
          prepared::MakeCanonical(canonical);
      // Check if statement should be served via a view or directly.
      if (prepared::NeedsFlow(canonical)) {
        // Plan the query.
        std::string flow_name = "_" + std::to_string(stmts.size());
        std::string create_view_stmt =
            "CREATE VIEW " + flow_name + " AS '\"" + canonical + "\"'";
        CHECK_STATUS_OR(exec(connection, create_view_stmt));
        // Query is planned: fill in the ? types.
        const auto &graph =
            connection->state->dataflow_state()->GetFlow(flow_name);
        prepared::FromFlow(flow_name, graph, &descriptor);
      } else {
        prepared::FromTables(canonical, *connection->state->dataflow_state(),
                             &descriptor);
      }
      // Store descriptor for future queries.
      stmts.emplace(canonical, std::move(descriptor));
    }
    // Downgrade the lock.
    reader_lock = shards::SharedLock(std::move(upgraded));
  }

  // Canonical statement must exist here.
  stmt.stmt_id = connection->stmts.size();
  stmt.canonical = &stmts.at(canonical);
  connection->stmts.emplace(stmt.stmt_id, std::move(stmt));
  return &connection->stmts.at(connection->stmts.size() - 1);
}

absl::StatusOr<SqlResult> exec(Connection *connection, size_t stmt_id,
                               const std::vector<std::string> &args) {
  const prepared::PreparedStatementDescriptor &stmt =
      connection->stmts.at(stmt_id);
  return exec(connection, prepared::PopulateStatement(stmt, args));
}

}  // namespace pelton
