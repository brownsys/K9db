// Defines the API for our SQLite-adapter.
#ifndef PELTON_PELTON_H_
#define PELTON_PELTON_H_

#include <mutex>
#include <string>

#include "absl/status/statusor.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sql/result.h"

namespace pelton {

// Expose our mysql-like API to host applications.
using SqlResult = sql::SqlResult;
using SqlResultSet = sql::SqlResultSet;
using Schema = dataflow::SchemaRef;
using Record = dataflow::Record;

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

  void LockMutex() { this->mtx.lock(); }
  void UnlockMutex() { this->mtx.unlock(); }

 private:
  shards::SharderState sharder_state_;
  dataflow::DataFlowState dataflow_state_;
  std::string path_;
  std::mutex mtx;
};

struct Connection {
  State *pelton_state;
  void lock_mtx() { this->pelton_state->LockMutex(); };
  void unlock_mtx() { this->pelton_state->UnlockMutex(); };
};

// initialize pelton_state
bool initialize(const std::string &directory, const std::string &db_name,
                const std::string &db_username, const std::string &db_password);

// delete pelton_state
bool shutdown(bool shutdown_planner = true);

// open connection
bool open(Connection *connection);

absl::StatusOr<SqlResult> exec(Connection *connection, std::string sql);

// close connection
bool close(Connection *connection);

// Call this if you are certain you are not going to make more calls to
// make_view to shutdown the JVM.
void shutdown_planner();

}  // namespace pelton

#endif  // PELTON_PELTON_H_
