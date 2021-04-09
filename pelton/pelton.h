// Defines the API for our SQLite-adapter.
#ifndef PELTON_PELTON_H_
#define PELTON_PELTON_H_

#include <functional>
#include <string>

#include "absl/status/statusor.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/types.h"

namespace pelton {

class Connection {
 public:
  Connection() = default;

  // Not copyable or movable.
  Connection(const Connection &) = delete;
  Connection &operator=(const Connection &) = delete;
  Connection(const Connection &&) = delete;
  Connection &operator=(const Connection &&) = delete;

  void Initialize(const std::string &path) {
    this->path_ = path;
    if (this->path_.size() > 0 && this->path_.back() != '/') {
      this->path_ += "/";
    }
  }

  // Getters.
  shards::SharderState *GetSharderState() { return &this->sharder_state_; }
  dataflow::DataFlowState *GetDataFlowState() { return &this->dataflow_state_; }

  // State persistance.
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

 private:
  shards::SharderState sharder_state_;
  dataflow::DataFlowState dataflow_state_;
  std::string path_;
};

bool open(const std::string &directory, const std::string &db_username,
          const std::string &db_password, Connection *connection);

absl::StatusOr<SqlResult> exec(Connection *connection, std::string sql);

bool close(Connection *connection);

// Call this if you are certain you are not going to make more calls to
// make_view to shutdown the JVM.
void shutdown_planner();

}  // namespace pelton

#endif  // PELTON_PELTON_H_
