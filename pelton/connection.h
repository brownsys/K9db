#ifndef PELTON_CONNECTION_H_
#define PELTON_CONNECTION_H_

#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/sql/lazy_executor.h"

namespace pelton {

class State {
 public:
  State(size_t workers) : sharder_state_(), dataflow_state_(workers) {}

  // Not copyable or movable.
  State(const State &) = delete;
  State &operator=(const State &) = delete;
  State(const State &&) = delete;
  State &operator=(const State &&) = delete;

  // Getters.
  shards::SharderState *GetSharderState() { return &this->sharder_state_; }
  dataflow::DataFlowState *GetDataFlowState() { return &this->dataflow_state_; }

  void PrintSizeInMemory() const { this->dataflow_state_.PrintSizeInMemory(); }

 private:
  shards::SharderState sharder_state_;
  dataflow::DataFlowState dataflow_state_;
};

struct Connection {
  // Global pelton state that persists across open connections
  State *pelton_state;
  // Connection to the underlying databases.
  sql::SqlLazyExecutor executor_;
};

}  // namespace pelton

#endif  // PELTON_CONNECTION_H_
