#ifndef PELTON_CONNECTION_H_
#define PELTON_CONNECTION_H_

#include <memory>
// NOLINTNEXTLINE
#include <mutex>
// NOLINTNEXTLINE
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "pelton/dataflow/state.h"
#include "pelton/prepared.h"
#include "pelton/shards/state.h"
#include "pelton/sql/connection.h"
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
  std::vector<std::string> Initialize(const std::string &db_name,
                                      const std::string &db_path);

  // Accessors for underlying components states.
  shards::SharderState &SharderState() { return this->sstate_; }
  const shards::SharderState &SharderState() const { return this->sstate_; }
  dataflow::DataFlowState &DataflowState() { return this->dstate_; }
  const dataflow::DataFlowState &DataflowState() const { return this->dstate_; }

  // Accessors for the underlying database.
  sql::Connection *Database() { return this->database_.get(); }

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
  sql::SqlResult ListIndices() const;

  // Locks.
  util::UniqueLock WriterLock();
  util::SharedLock ReaderLock() const;
  util::SharedLock CanonicalReaderLock() const;

 private:
  // Component states.
  shards::SharderState sstate_;
  dataflow::DataFlowState dstate_;
  // Connection to underlying database.
  std::unique_ptr<sql::Connection> database_;
  // Descriptors of queries already encountered in canonical form.
  std::unordered_map<std::string, prepared::CanonicalDescriptor> stmts_;
  // Lock for managing stmts_.
  mutable util::UpgradableMutex mtx_;
  mutable util::UpgradableMutex canonical_mtx_;
};

struct Connection {
  // Global pelton state that persists across open connections
  State *state;
  // Prepared statements created by this connection.
  std::vector<prepared::PreparedStatementDescriptor> stmts;
  std::unique_ptr<sql::Session> session;
};

}  // namespace pelton

#endif  // PELTON_CONNECTION_H_
