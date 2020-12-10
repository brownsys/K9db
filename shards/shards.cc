// Defines the API for our SQLite-adapter.
#include "shards/shards.h"

#include <iostream>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "shards/sqlengine/engine.h"
#include "shards/sqlengine/util.h"

namespace shards {

namespace {

// String whitespace trimming.
// https://stackoverflow.com/questions/216823/whats-the-best-way-to-trim-stdstring/25385766
static inline void LTrim(std::string &s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
            return !std::isspace(ch);
          }));
}
static inline void RTrim(std::string &s) {
  s.erase(std::find_if(s.rbegin(), s.rend(),
                       [](unsigned char ch) { return !std::isspace(ch); })
              .base(),
          s.end());
}
static inline void Trim(std::string &s) {
  LTrim(s);
  RTrim(s);
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
  Trim(sql);
  if (echo) {
    std::cout << sql << std::endl;
  }
  if (SpecialStatements(sql, state)) {
    return true;
  }

  for (const auto &[shard_suffix, sql_statement, modifier] :
       sqlengine::Rewrite(sql, state)) {
    if (log) {
      std::cout << "Shard: " << shard_suffix << std::endl;
      std::cout << "Statement: " << sql_statement << std::endl;
    }
    bool result = state->pool_.ExecuteStatement(shard_suffix, sql_statement,
                                                modifier, errmsg);
    if (!result) {
      // TODO(babman): we probably need some *transactional* notion here
      // about failures.
      return false;
    }
  }
  state->pool_.FlushBuffer(callback, context, errmsg);
  return true;
}

bool close(SharderState *state) {
  state->Save();
  return true;
}

}  // namespace shards
