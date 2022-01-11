// Defines the API for our SQLite-adapter.
#ifndef PELTON_PELTON_H_
#define PELTON_PELTON_H_

#include <string>

#include "absl/status/statusor.h"
#include "pelton/connection.h"
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

// initialize pelton_state
bool initialize(size_t workers, bool consistent);

// delete pelton_state
bool shutdown(bool shutdown_jvm);
bool shutdown();

// open connection
bool open(Connection *connection, const std::string &db_name);

absl::StatusOr<SqlResult> exec(Connection *connection, std::string sql);

// close connection
bool close(Connection *connection);

// Call this if you are certain you are not going to make more calls to
// make_view to shutdown the JVM.
void shutdown_planner();
void shutdown_planner(bool shutdown_jvm);

}  // namespace pelton

#endif  // PELTON_PELTON_H_
