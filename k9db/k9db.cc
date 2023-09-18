// Defines the API for our SQLite-adapter.
#include "k9db/k9db.h"

#include <iostream>
#include <memory>
#include <optional>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "glog/logging.h"
#include "k9db/dataflow/graph.h"
#include "k9db/explain.h"
#include "k9db/planner/planner.h"
#include "k9db/shards/sqlengine/engine.h"
#include "k9db/sqlast/command.h"
#include "k9db/util/status.h"
#include "k9db/util/upgradable_lock.h"

namespace k9db {

namespace {

// global state that persists between client connections
static State *K9DB_STATE = nullptr;

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
bool auto_ctx = false;

absl::StatusOr<std::optional<SqlResult>> SpecialStatements(
    const std::string &sql, Connection *connection) {
  if (absl::StartsWith(sql, "SET ")) {
    std::vector<std::string> split = absl::StrSplit(sql, ' ');
    if (absl::StartsWith(split.at(1), "echo")) {
      echo = true;
      return SqlResult(true);
    }
    if (absl::StartsWith(split.at(1), "AUTO_CTX")) {
      auto_ctx = true;
      CHECK_STATUS(connection->ctx->Start());
      return SqlResult(true);
    }
  }
  if (absl::StartsWith(sql, "EXPLAIN COMPLIANCE")) {
    explain::ExplainCompliance(*connection);
    return SqlResult(true);
  }
  if (absl::StartsWith(sql, "EXPLAIN ")) {
    std::string query = sql.substr(8);
    // Check if this matches a previously prepared statement with some view.
    State *state = connection->state;
    util::SharedLock lock = state->CanonicalReaderLock();
    auto pair = prepared::Canonicalize(query);
    if (state->HasCanonicalStatement(pair.first)) {
      const prepared::CanonicalDescriptor &desc =
          state->GetCanonicalStatement(pair.first);
      if (desc.view_name.has_value()) {
        const auto &schema = dataflow::SchemaFactory::EXPLAIN_QUERY_SCHEMA;
        std::vector<dataflow::Record> v;
        v.emplace_back(schema, true, std::make_unique<std::string>("VIEW"),
                       std::make_unique<std::string>(desc.view_name.value()));
        return sql::SqlResult(sql::SqlResultSet(schema, std::move(v)));
      }
    }
    return std::optional<SqlResult>{};
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
    if (absl::StartsWith(split.at(1), "PREPARED")) {
      return connection->state->PreparedDebug();
    }
    if (absl::StartsWith(split.at(1), "INDICES")) {
      return connection->state->ListIndices();
    }
  }
  if (absl::StartsWith(sql, "CTX")) {
    std::vector<std::string> split = absl::StrSplit(sql, ' ');
    if (absl::StartsWith(split.at(1), "START")) {
      CHECK_STATUS(connection->ctx->Start());
      return sql::SqlResult(true);
    }
    if (absl::StartsWith(split.at(1), "COMMIT")) {
      CHECK_STATUS(connection->ctx->Commit());
      return sql::SqlResult(true);
    }
    if (absl::StartsWith(split.at(1), "ROLLBACK")) {
      CHECK_STATUS(connection->ctx->Discard());
      return sql::SqlResult(true);
    }
  }
  return std::optional<SqlResult>{};
}

}  // namespace

bool initialize(size_t workers, bool consistent, const std::string &db_name,
                const std::string &db_path) {
  // if already open
  if (K9DB_STATE != nullptr) {
    return false;
  }

  // Initialize the global state including the rocksdb interface.
  K9DB_STATE = new State(workers, consistent);
  std::vector<std::string> stmts = K9DB_STATE->Initialize(db_name, db_path);

  // Create a temporary session in order to reload the pre-existing tables and
  // views.
  Connection connection;
  if (!open(&connection)) {
    return false;
  }

  // Re-execute previously-persisted create table, index, and view statements.
  for (std::string stmt : stmts) {
    MOVE_OR_PANIC(SqlResult result, exec(&connection, stmt));
    if (!result.Success()) {
      return false;
    }
  }

  // Close temporary session.
  if (!close(&connection)) {
    return false;
  }

  // Done!
  return true;
}

bool open(Connection *connection) {
  // set global state in local connection struct
  connection->state = K9DB_STATE;
  connection->session = K9DB_STATE->Database()->OpenSession();
  connection->ctx =
      std::make_unique<ComplianceTransaction>(connection->session.get());
  if (auto_ctx) {
    PANIC(connection->ctx->Start());
  }
  return true;
}

bool close(Connection *connection) {
  PANIC(connection->ctx->Commit());
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
  MOVE_OR_RETURN(auto special, SpecialStatements(sql, connection));
  if (special) {
    return std::move(special.value());
  }

  // Check if this is a query that can only be served with a view.
  if (prepared::NeedsFlow(sql)) {
    try {
      // Create a one-off view for this query.
      if (sql.back() == ';') {
        sql.pop_back();
      }

      size_t idx = connection->state->GetAndIncrementOneOffViewCount();
      std::string flow_name = "_oneoff_" + std::to_string(idx);
      sqlast::SQLCommand create_view_stmt("CREATE VIEW " + flow_name +
                                          " AS '\"" + sql + "\"'");

      // Make sure creation was successful.
      MOVE_OR_RETURN(sql::SqlResult result,
                     shards::sqlengine::Shard(create_view_stmt, connection));
      ASSERT_RET(result.Success(), Internal, "Could not create one-off view");

      // Select the content of the view and return it.
      sqlast::SQLCommand select_view("SELECT * FROM " + flow_name + ";");
      return shards::sqlengine::Shard(select_view, connection);
    } catch (std::exception &e) {
      return absl::InternalError(e.what());
    }
  }

  // Parse and rewrite statement.
  try {
    sqlast::SQLCommand command(std::move(sql));
    auto result = shards::sqlengine::Shard(command, connection);
    if (!result.ok()) {
      connection->session->RollbackTransaction();
    }
    return result;
  } catch (std::exception &e) {
    return absl::InternalError(e.what());
  }
}

void shutdown_planner(bool shutdown_jvm) {
  planner::ShutdownPlanner(shutdown_jvm);
}

bool shutdown(bool shutdown_jvm) {
  if (K9DB_STATE != nullptr) {
    shutdown_planner(shutdown_jvm);
    delete K9DB_STATE;
    K9DB_STATE = nullptr;
    return true;
  }
  return false;
}

// Prepared Statement API.
absl::StatusOr<const PreparedStatement *> prepare(Connection *connection,
                                                  const std::string &query) {
  // Acquire a reader shared lock.
  State *state = connection->state;
  util::SharedLock lock = state->CanonicalReaderLock();

  // Canonicalize the query.
  auto pair = prepared::Canonicalize(query);
  prepared::CanonicalQuery &canonical = pair.first;
  std::vector<size_t> &arg_value_count = pair.second;

  // Upgrade lock to a writer lock if this is the first time we encounter the
  // canonical statement, as we will need to modify the global state prepared
  // statement mapping.
  auto &&[upgraded, condition] = lock.UpgradeIf(
      [&]() { return !state->HasCanonicalStatement(canonical); });
  if (condition) {
    // We upgraded to a writer lock, we also know that the canonical statement
    // was never encountered.
    char c = query[0];
    if (c == 'I' || c == 'i' || c == 'R' || c == 'r') {
      // Handling insert is almost trivial.
      // We need to parse the statement, and find all ?.
      // For each ?, we build the appropriate stem and find the corresponding
      // column type.
      const auto &dstate = state->DataflowState();
      MOVE_OR_RETURN(prepared::CanonicalDescriptor descriptor,
                     prepared::MakeInsertCanonical(canonical, dstate));
      // Store descriptor for future queries.
      state->AddCanonicalStatement(canonical, std::move(descriptor));
    } else {
      prepared::CanonicalDescriptor descriptor =
          prepared::MakeCanonical(canonical);
      // Check if statement should be served via a view or directly.
      if (prepared::NeedsFlow(canonical)) {
        // Plan the query.
        std::string flow_name =
            "_" + std::to_string(state->CanonicalStatementCount());
        std::string create_view_stmt =
            "CREATE VIEW " + flow_name + " AS '\"" + canonical + "\"'";
        CHECK_STATUS_OR(exec(connection, create_view_stmt));
        // Query is planned: fill in the ? types.
        const auto &graph = state->DataflowState().GetFlow(flow_name);
        prepared::FromFlow(flow_name, graph, &descriptor);
      } else {
        prepared::FromTables(canonical, state->DataflowState(), &descriptor);
      }
      // Store descriptor for future queries.
      state->AddCanonicalStatement(canonical, std::move(descriptor));
    }
  }

  // Canonicalized query was handled before (either previous query, or by above
  // if statement). Now we can avoid duplicating views.
  const prepared::CanonicalDescriptor *ptr =
      &state->GetCanonicalStatement(canonical);

  // Canonical statement must exist here.
  // Extract information about the count of each parameter.
  prepared::PreparedStatementDescriptor stmt =
      prepared::MakeStmt(query, ptr, std::move(arg_value_count));
  stmt.stmt_id = connection->stmts.size();
  connection->stmts.push_back(std::move(stmt));

  return &connection->stmts.back();
}

absl::StatusOr<SqlResult> exec(Connection *connection, size_t stmt_id,
                               const std::vector<std::string> &args) {
  const prepared::PreparedStatementDescriptor &stmt =
      connection->stmts.at(stmt_id);
  try {
    sqlast::SQLCommand sql = prepared::PopulateStatement(stmt, args);
    return shards::sqlengine::Shard(sql, connection);
  } catch (std::exception &e) {
    return absl::InternalError(e.what());
  }
}

}  // namespace k9db
