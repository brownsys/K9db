#ifndef PELTON_CONNECTION_H_
#define PELTON_CONNECTION_H_

#include <string>

#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/upgradable_lock.h"
#include "pelton/sql/lazy_executor.h"
#include "pelton/sql/result.h"

namespace pelton {

class State {
 public:
  explicit State(size_t workers) : sharder_state_(), dataflow_state_(workers) {}

  // Not copyable or movable.
  State(const State &) = delete;
  State &operator=(const State &) = delete;
  State(const State &&) = delete;
  State &operator=(const State &&) = delete;

  // Getters.
  shards::SharderState *sharder_state() { return &this->sharder_state_; }
  dataflow::DataFlowState *dataflow_state() { return &this->dataflow_state_; }

  // Statistics.
  sql::SqlResult FlowDebug(const std::string &view_name) const {
    shards::SharedLock lock = this->sharder_state_.ReaderLock();
    return this->dataflow_state_.FlowDebug(view_name);
  }
  sql::SqlResult SizeInMemory() const {
    shards::SharedLock lock = this->sharder_state_.ReaderLock();
    return this->dataflow_state_.SizeInMemory();
  }
  sql::SqlResult NumShards() const {
    shards::SharedLock lock = this->sharder_state_.ReaderLock();
    return this->sharder_state_.NumShards();
  }

 private:
  shards::SharderState sharder_state_;
  dataflow::DataFlowState dataflow_state_;
};

struct Connection {
  // Global pelton state that persists across open connections
  State *state;
  // Connection to the underlying databases.
  sql::SqlLazyExecutor executor;
};

}  // namespace pelton

#endif  // PELTON_CONNECTION_H_
