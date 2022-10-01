#ifndef PELTON_CONNECTION_H_
#define PELTON_CONNECTION_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "pelton/dataflow/state.h"
#include "pelton/prepared.h"
#include "pelton/shards/state.h"
#include "pelton/sql/abstract_connection.h"
#include "pelton/sql/result.h"
#include "pelton/util/upgradable_lock.h"

namespace pelton {

class State {
 public:
  State(size_t workers, bool consistent);
  ~State();

  // Not copyable or movable.
  State(const State &) = delete;
  State &operator=(const State &) = delete;
  State(const State &&) = delete;
  State &operator=(const State &&) = delete;

  // Initialize when db name is known.
  void Initialize(const std::string &db_name);

  // Accessors for underlying components states.
  shards::SharderState &SharderState() { return this->sstate_; }
  const shards::SharderState &SharderState() const { return this->sstate_; }
  dataflow::DataFlowState &DataflowState() { return this->dstate_; }
  const dataflow::DataFlowState &DataflowState() const { return this->dstate_; }

  // Accessors for the underlying database.
  sql::AbstractConnection *Database() { return this->database_.get(); }

  // Manage cannonical prepared statements.
  bool HasCanonicalStatement(const std::string &canonical) const;
  size_t CanonicalStatementCount() const;
  const prepared::CanonicalDescriptor &GetCanonicalStatement(
      const std::string &canonical) const;
  void AddCanonicalStatement(const std::string &canonical,
                             prepared::CanonicalDescriptor &&descriptor);

  // Statistics.
  sql::SqlResult FlowDebug(const std::string &view_name) const;
  sql::SqlResult SizeInMemory() const;
  sql::SqlResult NumShards() const;
  sql::SqlResult PreparedDebug() const;

  // Locks.
  util::UniqueLock WriterLock();
  util::SharedLock ReaderLock() const;

 private:
  // Component states.
  shards::SharderState sstate_;
  dataflow::DataFlowState dstate_;
  // Connection to underlying database.
  std::unique_ptr<sql::AbstractConnection> database_;
  // Descriptors of queries already encountered in canonical form.
  std::unordered_map<std::string, prepared::CanonicalDescriptor> stmts_;
  // Lock for managing stmts_.
  mutable util::UpgradableMutex mtx_;
};

struct Connection {
  // Global pelton state that persists across open connections
  State *state;
  // Prepared statements created by this connection.
  std::vector<prepared::PreparedStatementDescriptor> stmts;
};

}  // namespace pelton

#endif  // PELTON_CONNECTION_H_
