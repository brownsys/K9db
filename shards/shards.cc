// Defines the API for our SQLite-adapter.
#include "shards/shards.h"

#include <iostream>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "dataflow/graph.h"
#include "dataflow/ops/filter.h"
#include "dataflow/ops/input.h"
#include "dataflow/schema.h"
#include "shards/sqlengine/engine.h"
#include "shards/sqlengine/util.h"
#include "shards/sqlexecutor/executor.h"

namespace shards {

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
bool log = false;
bool echo = false;

bool SpecialStatements(const std::string &sql, SharderState *state) {
  if (sql == "SET verbose;") {
    log = true;
    return true;
  }
  if (sql == "SET echo;") {
    echo = true;
    std::cout << "SET echo;" << std::endl;
    return true;
  }
  if (absl::StartsWith(sql, "GET ")) {
    std::vector<std::string> v = absl::StrSplit(sql, ' ');
    v.at(2).pop_back();
    std::string shard_name = sqlengine::NameShard(v.at(1), v.at(2));
    std::cout << absl::StrCat(state->dir_path(), shard_name, ".sqlite3")
              << std::endl;
    return true;
  }
  return false;
}

}  // namespace

bool open(const std::string &directory, SharderState *state) {
  state->Initialize(directory);
  state->Load();
  return true;
}

bool exec(SharderState *state, std::string sql, Callback callback,
          void *context, char **errmsg) {
  // Trim statement.
  Trim(sql);
  if (echo) {
    std::cout << sql << std::endl;
  }

  // If special statement, handle it separately.
  if (SpecialStatements(sql, state)) {
    return true;
  }

  // Parse and rewrite statement.
  auto statusor = sqlengine::Rewrite(sql, state);
  if (!statusor.ok()) {
    std::cout << statusor.status() << std::endl;
    return false;
  }

  // Successfully re-written into a list of modified statements.
  state->SQLExecutor()->StartBlock();
  for (auto &executable_statement : statusor.value()) {
    if (log) {
      std::cout << "Shard: " << executable_statement->shard_suffix()
                << std::endl;
      std::cout << "Statement: " << executable_statement->sql_statement()
                << std::endl;
    }
    bool result = state->SQLExecutor()->ExecuteStatement(
        std::move(executable_statement), callback, context, errmsg);
    if (!result) {
      // TODO(babman): we probably need some *transactional* notion here
      // about failures.
      return false;
    }
  }
  return true;
}

bool close(SharderState *state) {
  state->Save();
  return true;
}

// Materialized views.
#include "shards/todo.inc"

}  // namespace shards
