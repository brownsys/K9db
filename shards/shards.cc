// Defines the API for our SQLite-adapter.
#include "shards/shards.h"

#include "shards/sqlengine/engine.h"

namespace shards {

bool open(const std::string &directory, SharderState *state) {
  state->Initialize(directory);
  return true;
}

bool exec(SharderState *state, const std::string &sql, Callback callback,
          void *context, char **errmsg) {
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
