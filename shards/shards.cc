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

bool SpecialStatements(const std::string &sql, SharderState *state) {
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
  return true;
}

bool exec(SharderState *state, const std::string &sql, Callback callback,
          void *context, char **errmsg) {
  if (SpecialStatements(sql, state)) {
    return true;
  }

  for (const auto &[shard_suffix, sql_statement, modifier] :
       sqlengine::Rewrite(sql, state)) {
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

bool close(SharderState *state) { return true; }

}  // namespace shards
