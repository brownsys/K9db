#ifndef PELTON_CONNECTION_H_
#define PELTON_CONNECTION_H_

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "pelton/dataflow/state.h"
#include "pelton/prepared.h"
#include "pelton/shards/state.h"
#include "pelton/shards/upgradable_lock.h"
#include "pelton/sql/executor.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

namespace pelton {

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
  std::unordered_map<std::string, prepared::CanonicalDescriptor> &stmts() {
    return this->stmts_;
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
  sql::SqlResult PreparedDebug() const {
    // Acquire reader lock.
    shards::SharedLock reader_lock = this->ReaderLock();
    // Create schema.
    dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(
        {"Statement"}, {sqlast::ColumnDefinition::Type::TEXT}, {0});
    // Get all canonicalized statements and sort them.
    std::vector<std::string> vec;
    for (const auto &[canonical, _] : this->stmts_) {
      vec.push_back(canonical);
    }
    std::sort(vec.begin(), vec.end());
    // Create records.
    std::vector<dataflow::Record> records;
    for (const auto &canonical : vec) {
      records.emplace_back(schema, true,
                           std::make_unique<std::string>(canonical));
    }
    // Return result.
    return sql::SqlResult(sql::SqlResultSet(schema, std::move(records)));
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
  // Descriptors of queries already encountered in canonical form.
  std::unordered_map<std::string, prepared::CanonicalDescriptor> stmts_;
  // Lock for managing stmts_.
  mutable shards::UpgradableMutex mtx_;
};

struct Connection {
  // Global pelton state that persists across open connections
  State *state;
  // Connection to the underlying databases.
  sql::PeltonExecutor executor;
  // Prepared statements created by this connection.
  std::unordered_map<size_t, prepared::PreparedStatementDescriptor> stmts;
};

}  // namespace pelton

#endif  // PELTON_CONNECTION_H_
