// Defines the API for our SQLite-adapter.
#include "pelton/pelton.h"

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
bool log = false;
bool echo = false;

bool SpecialStatements(const std::string &sql, Connection *connection) {
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
    std::string shard_name = shards::sqlengine::NameShard(v.at(1), v.at(2));
    const std::string &dir_path = connection->GetSharderState()->dir_path();
    std::cout << absl::StrCat(dir_path, shard_name, ".sqlite3") << std::endl;
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

bool exec(Connection *connection, std::string sql, Callback callback,
          void *context, char **errmsg) {
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
  auto statusor =
      shards::sqlengine::Rewrite(sql, connection->GetSharderState());
  if (!statusor.ok()) {
    std::cout << statusor.status() << std::endl;
    return false;
  }

  // Successfully re-written into a list of modified statements.
  connection->GetSharderState()->SQLExecutor()->StartBlock();
  for (auto &executable_statement : statusor.value()) {
    if (log) {
      std::cout << "Shard: " << executable_statement->shard_suffix()
                << std::endl;
      std::cout << "Statement: " << executable_statement->sql_statement()
                << std::endl;
    }
    bool result =
        connection->GetSharderState()->SQLExecutor()->ExecuteStatement(
            std::move(executable_statement), callback, context, errmsg);
    if (!result) {
      // TODO(babman): we probably need some *transactional* notion here
      // about failures.
      return false;
    }
  }
  return true;
}

bool close(Connection *connection) {
  connection->Save();
  return true;
}

// Materialized views.
#include "pelton/todo.inc"

}  // namespace pelton
