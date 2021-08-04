// Defines the API for our SQLite-adapter.
#ifndef PELTON_PELTON_H_
#define PELTON_PELTON_H_

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

class Connection {
 public:
  Connection() = default;

  // Not copyable or movable.
  Connection(const Connection &) = delete;
  Connection &operator=(const Connection &) = delete;
  Connection(const Connection &&) = delete;
  Connection &operator=(const Connection &&) = delete;

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

  // Won't need save and load to save to files. Eventually will delete these::
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

  uint64_t SizeInMemory() const { return this->dataflow_state_.SizeInMemory(); }

 private:
  shards::SharderState sharder_state_;
  dataflow::DataFlowState dataflow_state_;
  std::string path_;
};

class ConnectionLocal {
 public:
  Connection *global_connection;
};

// open persistent connection, initialize pelton_state
bool global_open(const std::string &directory, const std::string &db_name,
                 const std::string &db_username,
                 const std::string &db_password);

// close global connection, delete pelton_state
bool global_close(bool shutdown_planner = true);

// open local connection
bool open(ConnectionLocal *connection);

absl::StatusOr<SqlResult> exec(ConnectionLocal *connection, std::string sql);

// close local connection
bool close(ConnectionLocal *connection);

// Call this if you are certain you are not going to make more calls to
// make_view to shutdown the JVM.
void shutdown_planner();

}  // namespace pelton

#endif  // PELTON_PELTON_H_
