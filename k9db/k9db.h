// Defines the API for our SQLite-adapter.
#ifndef K9DB_K9DB_H_
#define K9DB_K9DB_H_

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "k9db/connection.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/state.h"
#include "k9db/prepared.h"
#include "k9db/shards/state.h"
#include "k9db/shards/types.h"
#include "k9db/sql/result.h"

namespace k9db {

// Expose our mysql-like API to host applications.
using SqlResult = sql::SqlResult;
using SqlResultSet = sql::SqlResultSet;
using Schema = dataflow::SchemaRef;
using Record = dataflow::Record;
using PreparedStatement = prepared::PreparedStatementDescriptor;

// initialize k9db_state
bool initialize(size_t workers, bool consistent, const std::string &db_name,
                const std::string &db_path);

// delete k9db_state
bool shutdown(bool shutdown_jvm = true);

// open connection
bool open(Connection *connection);

absl::StatusOr<SqlResult> exec(Connection *connection, std::string sql);

// close connection
bool close(Connection *connection);

// Call this if you are certain you are not going to make more calls to
// make_view to shutdown the JVM.
void shutdown_planner(bool shutdown_jvm = true);

// Prepared Statement API.
absl::StatusOr<const PreparedStatement *> prepare(Connection *connection,
                                                  const std::string &query);

absl::StatusOr<SqlResult> exec(Connection *connection, size_t stmt_id,
                               const std::vector<std::string> &args);

}  // namespace k9db

#endif  // K9DB_K9DB_H_
