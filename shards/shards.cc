// Defines the API for our SQLite-adapter.
#include "shards/shards.h"

// libsqlite3-dev, -lsqlite3
//
#include <iostream>

#include "shards/sqlengine/engine.h"

namespace shards {

bool open(const std::string &directory, SharderState *state) {
  state->Initialize(directory);
  return true;
}

bool exec(SharderState *state, const std::string &sql, Callback callback,
          void *context, char **errmsg) {
  for (const auto &[shard_suffix, sql_statement] :
       sqlengine::Rewrite(sql, state)) {
    if (!state->ExecuteStatement(shard_suffix, sql_statement)) {
      // TODO(babman): we probably need some *transactional* notion here
      // about failures.
      return false;
    }
  }
  return true;
}

bool close(SharderState *state) { return true; }

}  // namespace shards
