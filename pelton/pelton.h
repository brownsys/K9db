// Defines the API for our SQLite-adapter.
#ifndef PELTON_PELTON_H_
#define PELTON_PELTON_H_

#include <functional>
#include <string>

#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"

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
  dataflow::DataFlowState *GetDataFlowState() { return &this->dataflow_state_; }

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
  dataflow::DataFlowState dataflow_state_;
};

bool open(const std::string &directory, Connection *connection);

bool exec(Connection *connection, std::string sql,
          const shards::Callback &callback, void *context, char **errmsg);

bool close(Connection *connection);

// Call this if you are certain you are not going to make more calls to
// make_view to shutdown the JVM.
void shutdown_planner();

}  // namespace pelton

#endif  // PELTON_PELTON_H_
