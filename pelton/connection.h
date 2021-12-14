#ifndef PELTON_CONNECTION_H_
#define PELTON_CONNECTION_H_

#include <atomic>
#include <string>

#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/upgradable_lock.h"
#include "pelton/sql/executor.h"
#include "pelton/sql/result.h"
#include "pelton/util/perf.h"

namespace pelton {

class State {
 public:
  explicit State(size_t workers, bool consistent)
      : sharder_state_(), dataflow_state_(workers, consistent) {
    for (size_t i = 0; i < 500; i++) {
      this->perfs_.push_back(perf::Perf());
    }
  }

  // Not copyable or movable.
  State(const State &) = delete;
  State &operator=(const State &) = delete;
  State(const State &&) = delete;
  State &operator=(const State &&) = delete;

  ~State() {
    this->dataflow_state_.Shutdown();
    sql::PeltonExecutor::CloseAll();
  }

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
  
  perf::Perf CombinePerfs() const {
    perf::Perf perf;
    for (const auto &p : this->perfs_) {
      perf.Combine(p);
    }
    return perf;
  }
  
  perf::Perf *GetPerf() {
    return &this->perfs_.at(this->atomic_++);
  }

 private:
  shards::SharderState sharder_state_;
  dataflow::DataFlowState dataflow_state_;
  std::vector<perf::Perf> perfs_;
  std::atomic<size_t> atomic_;
};

struct Connection {
  // Global pelton state that persists across open connections
  State *state;
  // Connection to the underlying databases.
  sql::PeltonExecutor executor;
  // Perf tracker.
  perf::Perf *perf;
};

}  // namespace pelton

#endif  // PELTON_CONNECTION_H_
