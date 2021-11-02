#ifndef PELTON_CONNECTION_H_
#define PELTON_CONNECTION_H_

#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/sql/lazy_executor.h"

namespace pelton {

class State {
 public:
  State() = default;

  // Not copyable or movable.
  State(const State &) = delete;
  State &operator=(const State &) = delete;
  State(const State &&) = delete;
  State &operator=(const State &&) = delete;

  // path to store state if want to store in folder (usually give it empty
  // string)
  void Initialize(const std::string &path) {
    this->path_ = path;
    if (this->path_.size() > 0 && this->path_.back() != '/') {
      this->path_ += "/";
    }
  }

  // Getters.
  shards::SharderState *GetSharderState() { return &this->sharder_state_; }
  dataflow::DataFlowState *GetDataFlowState() { return &this->dataflow_state_; }

  void Save() {
    if (this->path_.size() > 0) {
      this->sharder_state_.Save(this->path_);
      this->dataflow_state_.Save(this->path_);
    }
  }
  void Load() {
    if (this->path_.size() > 0) {
      this->sharder_state_.Load(this->path_);
      this->dataflow_state_.Load(this->path_);
    }
  }

  void PrintSizeInMemory() const { this->dataflow_state_.PrintSizeInMemory(); }

 private:
  shards::SharderState sharder_state_;
  dataflow::DataFlowState dataflow_state_;
  std::string path_;
};

struct Connection {
  // global pelton state that persists across open connections
  State *pelton_state;
  // Connection pool that manages the underlying databases.
  sql::SqlLazyExecutor executor_;
};

}  // namespace pelton

#endif  // PELTON_CONNECTION_H_