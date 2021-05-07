// Defines the API for our SQLite-adapter.
#include "pelton/pelton.h"

#include <iostream>
#include <utility>
#include <vector>

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
  return false;
}

}  // namespace

absl::StatusOr<std::vector<SqlResult>> gdpr(Connection *connection,
                                            std::string sql) {
  Trim(sql);
  if (echo) {
    std::cout << sql << std::endl;
  }

  shards::SharderState *state = connection->GetSharderState();
  dataflow::DataFlowState *dstate = connection->GetDataFlowState();

  std::vector<SqlResult> results;
  if (absl::StartsWith(sql, "GET ")) {
    std::vector<std::string> v = absl::StrSplit(sql, ' ');
    std::string &shard_kind = v.at(1);
    std::string &user_id = v.at(2);
    if (user_id.back() == ';') {
      user_id.pop_back();
    }

    for (const auto &[table, shard_by] : state->ShardTables(shard_kind)) {
      std::string tsql = "SELECT * FROM " + table + " WHERE " + shard_by +
                         " = " + user_id + ";";
      MOVE_OR_RETURN(SqlResult res,
                     shards::sqlengine::Shard(tsql, state, dstate));
      results.push_back(std::move(res));
    }
  } else if (absl::StartsWith(sql, "FORGET ")) {
    std::vector<std::string> v = absl::StrSplit(sql, ' ');
    std::string &shard_kind = v.at(1);
    std::string &user_id = v.at(2);
    if (user_id.back() == ';') {
      user_id.pop_back();
    }

    for (const auto &[table, shard_by] : state->ShardTables(shard_kind)) {
      std::string tsql =
          "DELETE FROM " + table + " WHERE " + shard_by + " = " + user_id + ";";
      MOVE_OR_RETURN(SqlResult res,
                     shards::sqlengine::Shard(tsql, state, dstate));
      results.push_back(std::move(res));
    }
  }

  return results;
}

bool open(const std::string &directory, const std::string &db_username,
          const std::string &db_password, Connection *connection) {
  connection->Initialize(directory);
  connection->GetSharderState()->Initialize(db_username, db_password);
  connection->Load();
  return true;
}

absl::StatusOr<SqlResult> exec(Connection *connection, std::string sql) {
  // Trim statement.
  Trim(sql);
  if (echo) {
    std::cout << sql << std::endl;
  }

  // If special statement, handle it separately.
  if (SpecialStatements(sql, connection)) {
    return SqlResult();
  }

  // Parse and rewrite statement.
  shards::SharderState *sstate = connection->GetSharderState();
  dataflow::DataFlowState *dstate = connection->GetDataFlowState();
  return shards::sqlengine::Shard(sql, sstate, dstate);
}

void shutdown_planner() { planner::ShutdownPlanner(); }

bool close(Connection *connection) {
  connection->Save();
  planner::ShutdownPlanner();
  return true;
}

}  // namespace pelton
