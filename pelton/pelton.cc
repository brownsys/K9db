// Defines the API for our SQLite-adapter.
#include "pelton/pelton.h"

#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "pelton/dataflow/graph.h"
#include "pelton/planner/planner.h"
#include "pelton/shards/sqlengine/engine.h"
#include "pelton/shards/sqlengine/util.h"

namespace pelton {

namespace {

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
  if (sql == "SET echo;") {
    echo = true;
    std::cout << "SET echo;" << std::endl;
    return true;
  }
  if (absl::StartsWith(sql, "GET ")) {
    std::vector<std::string> v = absl::StrSplit(sql, ' ');
    v.at(2).pop_back();
    std::string shard_name = shards::sqlengine::NameShard(v.at(1), v.at(2));
    const std::string &dir_path = connection->GetSharderState()->dir_path();
    std::cout << absl::StrCat(dir_path, shard_name) << std::endl;
    return true;
  }
  return false;
}

}  // namespace

bool open(const std::string &directory, Connection *connection) {
  connection->GetSharderState()->Initialize(directory);
  connection->Load();
  return true;
}

bool exec(Connection *connection, std::string sql,
          const shards::Callback &callback, void *context, char **errmsg) {
  // Trim statement.
  Trim(sql);
  if (echo) {
    std::cout << sql << std::endl;
  }

  // If special statement, handle it separately.
  if (SpecialStatements(sql, connection)) {
    return true;
  }

  // Parse and rewrite statement.
  shards::SharderState *sstate = connection->GetSharderState();
  dataflow::DataFlowState *dstate = connection->GetDataFlowState();
  shards::OutputChannel output = {callback, context, errmsg};
  absl::Status status = shards::sqlengine::Shard(sql, sstate, dstate, output);
  if (!status.ok()) {
    std::cout << status << std::endl;
    return false;
  }

  return true;
}

void make_view(Connection *connection, const std::string &name,
               const std::string &query) {
  // Plan the query using calcite and generate a concrete graph for it.
  dataflow::DataFlowGraph graph =
      planner::PlanGraph(connection->GetDataFlowState(), query);
  // Add The flow to state so that data is fed into it on INSERT/UPDATE/DELETE.
  connection->GetDataFlowState()->AddFlow(name, graph);
}

void shutdown_planner() { planner::ShutdownPlanner(); }

bool close(Connection *connection) {
  connection->Save();
  planner::ShutdownPlanner();
  return true;
}

// Materialized views.
#include "pelton/todo.inc"

}  // namespace pelton
