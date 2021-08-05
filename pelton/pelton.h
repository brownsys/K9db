// Defines the API for our SQLite-adapter.
#ifndef PELTON_PELTON_H_
#define PELTON_PELTON_H_

#include <string>

#include "absl/status/statusor.h"
#include "pelton/dataflow/engine.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
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

  void Initialize(const std::string &path) {
    this->path_ = path;
    if (this->path_.size() > 0 && this->path_.back() != '/') {
      this->path_ += "/";
    }
  }

  // Getters.
  shards::SharderState *GetSharderState() { return &this->sharder_state_; }
  dataflow::DataFlowEngine *GetDataFlowEngine() {
    return &this->dataflow_engine_;
  }

  // State persistance.
  void Save() {
    if (this->path_.size() > 0) {
      this->sharder_state_.Save(this->path_);
      this->dataflow_engine_.Save(this->path_);
    }
  }
  void Load() {
    if (this->path_.size() > 0) {
      this->sharder_state_.Load(this->path_);
      this->dataflow_engine_.Load(this->path_);
    }
  }

  uint64_t SizeInMemory() const {
    return this->dataflow_engine_.SizeInMemory();
  }

 private:
  shards::SharderState sharder_state_;
  dataflow::DataFlowEngine dataflow_engine_;
  std::string path_;
};

bool open(const std::string &directory, const std::string &db_name,
          const std::string &db_username, const std::string &db_password,
          Connection *connection);

absl::StatusOr<SqlResult> exec(Connection *connection, std::string sql);

bool close(Connection *connection, bool shutdown_planner = true);

// Call this if you are certain you are not going to make more calls to
// make_view to shutdown the JVM.
void shutdown_planner();

}  // namespace pelton

#endif  // PELTON_PELTON_H_
