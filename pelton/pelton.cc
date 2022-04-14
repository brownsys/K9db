// Defines the API for our SQLite-adapter.
#include "pelton/pelton.h"

#include <iostream>
#include <optional>
#include <utility>
#include <iomanip>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "pelton/dataflow/graph.h"
#include "pelton/planner/planner.h"
#include "pelton/shards/sqlengine/engine.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/shards/upgradable_lock.h"
#include "pelton/util/status.h"
#include "pelton/shards/types.h"

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


void PrintTransitivityChain(std::ostream &out, const shards::ShardingInformation &info, const shards::SharderState &state, const int indent, std::vector<const shards::UnshardedTableName *> *varshards) {
  const auto & transitive_infos = state.GetShardingInformationFor(info.next_table, info.shard_kind);
  if (transitive_infos.size() != 1) {
    for (const auto &info : state.GetShardingInformation(info.next_table)) {
      std::cout << info << std::endl;
    }
    LOG(FATAL) << (transitive_infos.size() == 0 ? "No" : "Too many") << " next shardings for table: " << info.next_table << ", shard kind: " << info.shard_kind << ", column: " << info.next_column << " found.";
  }
  const auto &tinfo = *transitive_infos.front();
  if (tinfo.is_varowned())
    varshards->push_back(&tinfo.next_table);
  out << std::setw(indent) << "" << "via " << info.next_table << "(" << tinfo.shard_by << ") resolved with index " << info.next_index_name << std::endl;

  if (tinfo.IsTransitive())
    PrintTransitivityChain(out, tinfo, state, indent, varshards);
}

void ExplainPrivacy(const Connection &connection) {
  const shards::SharderState &state = *connection.state->sharder_state();
  auto & out = std::cout;

  // explain accessors

  // eventually maybe number of active shards
  
  for (const auto & [table_name, _] : state.tables()) {
    // Eventually also include if there are sharded tables that have a default
    // table which is non-empty, shich could be a privacy violation
    bool is_sharded = state.IsSharded(table_name);
    out << table_name << ": ";

    if (!state.IsSharded(table_name)) {
      out << "unsharded" << std::endl << std::endl;
      continue;
    } else if (state.IsPII(table_name)) {
      out << "is PII" << std::endl << std::endl;
    } else {
      out << "sharded with" << std::endl;
    }

    const auto &sharding_infos = state.GetShardingInformation(table_name);
    std::vector<std::vector<const shards::UnshardedTableName *>> varown_chains {};
    for (const auto &info : sharding_infos) {
      std::vector<const shards::UnshardedTableName *> varown_chain {};
      if (info.is_varowned())
        varown_chain.push_back(&info.next_table);
      out << "  " << info.shard_by << " shards to " << info.shard_kind << std::endl;
      if (info.IsTransitive()) {
        PrintTransitivityChain(out, info, state, 4, &varown_chain);
        out << "    total transitive distance is " << info.distance_from_shard << std::endl;
      }
      if (varown_chain.size() > 0)
        varown_chains.push_back(varown_chain);
    }

    std::vector<const shards::UnshardedTableName *> * longest_varown_chain = nullptr;

    for (auto &varown_chain : varown_chains) {
      if (!longest_varown_chain || longest_varown_chain->size() < varown_chain.size()) 
        longest_varown_chain = &varown_chain;
    }

    int regular_shardings = sharding_infos.size() - varown_chains.size();
    int longest_varown_chain_size = longest_varown_chain ? longest_varown_chain->size() : 0;

    // Feedback section

    if (longest_varown_chain_size > 1) {
      out << "  [SEVERE] This table is variably sharded " << longest_varown_chain->size() << " times in sequence. This will create ";
      bool put_sym = false;
      for (const auto &varown_table : *longest_varown_chain) {
        if (put_sym)
          out << '*';
        else 
          put_sym = true;
        out << *varown_table;
      }
      out << " copies of records inserted into this table. This is likely not desired behavior, I suggest checking your `OWNS` annotations." << std::endl;
    } 
    if (varown_chains.size() > 1) {
      out << "  [Warning] This table is variably owned in multiple ways (via ";
      bool put_sym = false;
      for (const auto &varown_tables: varown_chains) {
        if (put_sym)
          out << " and ";
        else 
          put_sym = true;
        out << varown_tables.front();
      }
      out << "). This may not be desired behavior." << std::endl;
    } else if (varown_chains.size() == 1) {
      out << "  [Info] this table is variably owned (via " << *varown_chains.front().front() << ")" << std::endl;
    }
    if (longest_varown_chain_size == 1 && sharding_infos.size() > 1) {
      out << "  [Warning] This table is variably owned and also copied an additional " << regular_shardings << " times." << std::endl;
    } else if (regular_shardings > 5) {
      out << "  [Warning] This table is sharded " << regular_shardings << " times. This seem excessive, you may want to check your `OWNER` annotations." << std::endl;
    } else if (regular_shardings > 2) {
      out << "  [Info] This table is copied " << regular_shardings << " times" << std::endl;
    }
    out << std::endl;
  }
  out << "Summary end" << std::endl;
}

// Special statements we added to SQL.
bool echo = false;

std::optional<SqlResult> SpecialStatements(const std::string &sql,
                                           Connection *connection) {
  if (absl::StartsWith(sql, "SET ")) {
    std::vector<std::string> split = absl::StrSplit(sql, ' ');
    if (absl::StartsWith(split.at(1), "echo")) {
      echo = true;
      ExplainPrivacy(*connection);
      return SqlResult(true);
    }
    if (absl::StartsWith(split.at(1), "ENDPOINT")) {
      if (split.size() == 2) {
        connection->state->perf().AddEndPoint(std::move(connection->endpoint));
      } else {
        connection->endpoint.Initialize(split.at(2));
      }
      return SqlResult(true);
    }
    if (absl::StartsWith(split.at(1), "START")) {
      for (size_t i = 0; i < 10; i++) {
        std::cout << std::endl;
      }
      std::cout << "############# STARTING LOAD #############" << std::endl;
      return SqlResult(true);
    }
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
    if (absl::StartsWith(split.at(1), "PERF")) {
      return connection->state->PerfList();
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
    connection->endpoint.AddQuery(sql);
    auto result = shards::sqlengine::Shard(sql, connection);
    if (result.ok()) {
      connection->endpoint.DoneQuery(result.value());
    } else {
      connection->endpoint.DoneQuery(-5);
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
  connection->endpoint.AddQuery("prep:: " + query);
  // Acquire a reader shared lock.
  shards::SharedLock reader_lock = connection->state->ReaderLock();

  // Canonicalize the query.
  auto pair = prepared::Canonicalize(query);
  prepared::CanonicalQuery &canonical = pair.first;
  std::vector<size_t> &arg_value_count = pair.second;

  // Check if canonicalized query was handled before (avoids duplicating views).
  auto &stmts = connection->state->stmts();
  prepared::CanonicalDescriptor *ptr = nullptr;
  auto it = stmts.find(canonical);
  if (it == stmts.end()) {
    // Upgrade lock to a writer lock, we need to modify stmts.
    shards::UniqueLock upgraded(std::move(reader_lock));
    // Make sure another thread did not create the canonical statement while we
    // were upgrading.
    it = stmts.find(canonical);
    if (it == stmts.end()) {
      char c = query[0];
      if (c == 'I' || c == 'i' || c == 'R' || c == 'r') {
        // Handling insert is almost trivial.
        // We need to parse the statement, and find all ?.
        // For each ?, we build the appropriate stem and find the corresponding
        // column type.
        const auto &dstate = *connection->state->dataflow_state();
        MOVE_OR_RETURN(prepared::CanonicalDescriptor descriptor,
                       prepared::MakeInsertCanonical(canonical, dstate));
        // Store descriptor for future queries.
        auto pair = stmts.emplace(canonical, std::move(descriptor));
        ptr = &pair.first->second;
      } else {
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
        auto pair = stmts.emplace(canonical, std::move(descriptor));
        ptr = &pair.first->second;
      }
    } else {
      ptr = &it->second;
    }
    // Downgrade the lock.
    reader_lock = shards::SharedLock(std::move(upgraded));
  } else {
    ptr = &it->second;
  }

  // Canonical statement must exist here.
  // Extract information about the count of each parameter.
  prepared::PreparedStatementDescriptor stmt =
      prepared::MakeStmt(query, ptr, std::move(arg_value_count));
  stmt.stmt_id = connection->stmts.size();
  connection->stmts.push_back(std::move(stmt));
  connection->endpoint.DoneQuery(0);
  return &connection->stmts.back();
}

absl::StatusOr<SqlResult> exec(Connection *connection, size_t stmt_id,
                               const std::vector<std::string> &args) {
  const prepared::PreparedStatementDescriptor &stmt =
      connection->stmts.at(stmt_id);
  return exec(connection, prepared::PopulateStatement(stmt, args));
}

}  // namespace pelton
