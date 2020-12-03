// Defines the API for our SQLite-adapter.
#ifndef SHARDS_SHARDS_H_
#define SHARDS_SHARDS_H_

#include <string>

#include "shards/state.h"

namespace shards {

bool open(const std::string &directory, SharderState *state);

bool exec(SharderState *state, const std::string &sql, Callback callback,
          void *context, char **errmsg);

bool close(SharderState *state);

}  // namespace shards

#endif  // SHARDS_SHARDS_H_
