// Defines the API for our SQLite-adapter.
#ifndef PELTON_PELTON_H_
#define PELTON_PELTON_H_

#include <functional>
#include <string>

#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"

namespace pelton {

class Connection {
 public:
  Connection() = default;

  // Not copyable or movable.
  Connection(const Connection &) = delete;
  Connection &operator=(const Connection &) = delete;
  Connection(const Connection &&) = delete;
  Connection &operator=(const Connection &&) = delete;

  // Getters.
  shards::SharderState *GetSharderState() { return &this->sharder_state_; }
  dataflow::DataflowState *GetDataflowState() { return &this->dataflow_state_; }

  // State persistance.
  void Save() {
    this->sharder_state_.Save();
    this->dataflow_state_.Save(this->sharder_state_.dir_path());
  }
  void Load() {
    this->sharder_state_.Load();
    this->dataflow_state_.Load(this->sharder_state_.dir_path());
  }

 private:
  shards::SharderState sharder_state_;
  dataflow::DataflowState dataflow_state_;
};

// (context, col_count, col_data, col_name)
// https://www.sqlite.org/c3ref/exec.html
using Callback = std::function<int(void *, int, char **, char **)>;

bool open(const std::string &directory, Connection *connection);

bool exec(Connection *connection, std::string sql, Callback callback,
          void *context, char **errmsg);

bool close(Connection *connection);

// Materialized views.
void make_view(Connection *connection, const std::string &name,
               const std::string &query);

void print_view(Connection *connection, const std::string &name);

}  // namespace pelton

#endif  // PELTON_PELTON_H_
