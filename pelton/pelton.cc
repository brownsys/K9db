// Defines the API for our SQLite-adapter.
#include "pelton/pelton.h"

#include <iostream>
#include <optional>
// NOLINTNEXTLINE
#include <regex>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "pelton/planner/planner.h"
#include "pelton/shards/sqlengine/engine.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/shards/upgradable_lock.h"
#include "pelton/util/status.h"

namespace pelton {

namespace {

#define COLUMN_NAME "\\b([A-Za-z0-9_]+)"
#define EQ "\\s*=\\s*\\?"
#define IN "\\s+[Ii][Nn]\\s*"
#define MQ "\\(\\s*\\?\\s*((?:\\s*,\\s*\\?)*)\\)"

static std::regex PARAM_REGEX{COLUMN_NAME "(?:" EQ "|" IN MQ ")"};

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
absl::StatusOr<const PreparedStatementDescriptor *> prepare(
    Connection *connection, const std::string &query) {
  shards::SharedLock reader_lock = connection->state->ReaderLock();
  std::string canonical = "";
  // First, we turn query into canonical form and find out the parameters.
  std::string q = query;
  std::smatch sm;
  std::vector<size_t> parameter_value_count;
  std::vector<std::string> parameter_name;
  size_t total_value_count = 0;
  while (regex_search(q, sm, PARAM_REGEX)) {
    canonical.append(sm.prefix().str());
    auto cit = sm[1];
    auto qit = sm[2];
    for (auto it = cit.first; it != cit.second; ++it) {
      canonical.push_back(*it);
    }
    canonical.append(" = ?");
    // Find out the associated ? count.
    size_t parameter_count = 1;
    for (auto it = qit.first; it != qit.second; ++it) {
      if (*it == '?') {
        parameter_count++;
      }
    }
    total_value_count += parameter_count;
    parameter_value_count.push_back(parameter_count);
    parameter_name.push_back(cit.str());
    // Next match.
    q = sm.suffix().str();
  }
  canonical.append(q);
  if (canonical.back() == ';') {
    canonical.pop_back();
  }

  // Second, we check if canonical query had already been planner, and otherwise
  // plan it into a flow!
  auto &flows = connection->state->flows();
  auto it = flows.find(canonical);
  if (it == flows.end()) {
    shards::UniqueLock upgraded_lock(std::move(reader_lock));
    if (flows.count(canonical) == 0) {
      std::string flow_name = "_" + std::to_string(flows.size());
      std::string create_view_stmt =
          "CREATE VIEW " + flow_name + " AS '\"" + canonical + "\"'";
      CHECK_STATUS_OR(exec(connection, create_view_stmt));
      // Insert the flow information into state.
      const auto &graph =
          connection->state->dataflow_state()->GetFlow(flow_name);
      PreparedFlowDescriptor fdescriptor;
      fdescriptor.flow_name = std::move(flow_name);
      fdescriptor.key_names = graph.matview_key_names();
      fdescriptor.key_types = graph.matview_key_types();
      for (size_t i = 0; i < fdescriptor.key_names.size(); i++) {
        fdescriptor.key_name_to_index.emplace(fdescriptor.key_names.at(i), i);
      }
      flows.emplace(canonical, std::move(fdescriptor));
    }
    reader_lock = shards::SharedLock(std::move(upgraded_lock));
  }
  const PreparedFlowDescriptor &flow_descriptor = flows.at(canonical);

  // Now, instantiate the flow into a prepared statement.
  // The crucial difference is in the argument vs keys.
  // Each argument in the prepared statement corresponds to a flow key.
  // However, the same key can have multiple values (e.g. IN (v1, v2 ...)).
  size_t stmt_id = connection->stmts.size();
  PreparedStatementDescriptor descriptor;
  descriptor.stmt_id = stmt_id;
  descriptor.total_count = total_value_count;
  descriptor.arg_value_count = std::move(parameter_value_count);
  for (const auto &parameter_name : parameter_name) {
    descriptor.arg_flow_key.push_back(
        flow_descriptor.key_name_to_index.at(parameter_name));
  }
  descriptor.flow_descriptor = &flow_descriptor;
  connection->stmts.emplace(descriptor.stmt_id, std::move(descriptor));
  return &connection->stmts.at(stmt_id);
}

absl::StatusOr<SqlResult> exec(Connection *connection, StatementID stmt_id,
                               const std::vector<std::string> &args) {
  // Look up prepared statement descriptor.
  const PreparedStatementDescriptor &descriptor = connection->stmts.at(stmt_id);
  // Create the select statement.
  std::string stmt = "SELECT * FROM " + descriptor.flow_descriptor->flow_name;
  if (args.size() > 0) {
    stmt += " WHERE";
    size_t value_index = 0;
    for (size_t i = 0; i < descriptor.arg_flow_key.size(); i++) {
      if (i > 0) {
        stmt += " AND";
      }

      size_t key_index = descriptor.arg_flow_key.at(i);
      size_t value_count = descriptor.arg_value_count.at(i);
      const auto &colname = descriptor.flow_descriptor->key_names.at(key_index);
      if (value_count == 1) {  // Single value.
        stmt += " " + colname + " = " + args.at(value_index);
      } else {
        stmt += " " + colname + " IN (";
        for (size_t j = 0; j < value_count; j++) {
          if (j > 0) {
            stmt += ", ";
          }
          stmt += args.at(value_index + j);
        }
        stmt += ")";
      }
      value_index += value_count;
    }
  }
  return exec(connection, stmt);
}

}  // namespace pelton
