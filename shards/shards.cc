// Defines the API for our SQLite-adapter.
#include "shards/shards.h"

// libsqlite3-dev, -lsqlite3
// #include <sqlite3.h>
#include <iostream>

#include "shards/sqlengine/engine.h"

namespace shards {

bool open(const std::string &directory, SharderState *state) {
  state->Initialize(directory);
  return true;
}

bool exec(SharderState *state, const std::string &sql, Callback callback,
          void *context, char **errmsg) {
  sqlengine::Rewrite(sql, state);
  return true;
}

bool close(SharderState *state) { return true; }

}  // namespace shards
