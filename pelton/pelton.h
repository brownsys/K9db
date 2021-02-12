// Defines the API for our SQLite-adapter.
#ifndef PELTON_PELTON_H_
#define PELTON_PELTON_H_

#include <functional>
#include <string>

#include "shards/state.h"

namespace pelton {

// (context, col_count, col_data, col_name)
// https://www.sqlite.org/c3ref/exec.html
using Callback = std::function<int(void *, int, char **, char **)>;

bool open(const std::string &directory, shards::SharderState *state);

bool exec(shards::SharderState *state, std::string sql, Callback callback,
          void *context, char **errmsg);

bool close(shards::SharderState *state);

// Materialized views.
void make_view(shards::SharderState *state, const std::string &name,
               const std::string &query);

void print_view(shards::SharderState *state, const std::string &name);

}  // namespace pelton

#endif  // PELTON_PELTON_H_
