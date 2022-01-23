#ifndef PELTON_CONNECTION_H_
#define PELTON_CONNECTION_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/upgradable_lock.h"
#include "pelton/sql/executor.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

namespace pelton {

// Prepared statement API.
using StatementID = size_t;
struct PreparedFlowDescriptor {
  std::string flow_name;
  std::vector<std::string> key_names;
  std::vector<sqlast::ColumnDefinition::Type> key_types;
  std::unordered_map<std::string, size_t> key_name_to_index;
};
struct PreparedStatementDescriptor {
  StatementID stmt_id;
  size_t total_count;
  std::vector<size_t> arg_value_count;
  std::vector<size_t> arg_flow_key;
  // Points to corresponding flow descriptor.
  const PreparedFlowDescriptor *flow_descriptor;
};

class State {
 public:
  explicit State(size_t workers, bool consistent)
      : sharder_state_(), dataflow_state_(workers, consistent) {}

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
  std::unordered_map<std::string, PreparedFlowDescriptor> &flows() {
    return this->flows_;
  }

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

  // Locks.
  shards::UniqueLock WriterLock() { return shards::UniqueLock(&this->mtx_); }
  shards::SharedLock ReaderLock() const {
    return shards::SharedLock(&this->mtx_);
  }

 private:
  // Component states.
  shards::SharderState sharder_state_;
  dataflow::DataFlowState dataflow_state_;
  std::unordered_map<std::string, PreparedFlowDescriptor> flows_;
  mutable shards::UpgradableMutex mtx_;
};

struct Connection {
  // Global pelton state that persists across open connections
  State *state;
  // Connection to the underlying databases.
  sql::PeltonExecutor executor;
  // Prepared statements created by this connection.
  std::unordered_map<StatementID, PreparedStatementDescriptor> stmts;
};

}  // namespace pelton

#endif  // PELTON_CONNECTION_H_
