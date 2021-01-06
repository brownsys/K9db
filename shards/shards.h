// Defines the API for our SQLite-adapter.
#ifndef SHARDS_SHARDS_H_
#define SHARDS_SHARDS_H_

#include <functional>
#include <string>

#include "shards/state.h"

namespace shards {

// (context, col_count, col_data, col_name)
// https://www.sqlite.org/c3ref/exec.html
using Callback = std::function<int(void *, int, char **, char **)>;

bool open(const std::string &directory, SharderState *state);

bool exec(SharderState *state, std::string sql, Callback callback,
          void *context, char **errmsg);

bool close(SharderState *state);

}  // namespace shards

#endif  // SHARDS_SHARDS_H_
