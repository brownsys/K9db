#include "pelton/proxy/src/ffi/ffi.h"

#include <signal.h>
#include <stdlib.h>
#include <string.h>

#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "pelton/pelton.h"

DEFINE_uint32(workers, 1, "Number of workers");
DEFINE_bool(consistent, true, "Dataflow consistency with futures");
DEFINE_string(db_name, "pelton", "Name of the database");
DEFINE_string(hostname, "127.0.0.1:10001", "Hostname to bind against");

uint64_t ffi_total_time = 0;
uint64_t ffi_total_count = 0;

// Defined in rust (proxy.rs).
extern "C" {
void proxy_terminate(int);
}

// process command line arguments with gflags
FFIArgs FFIGflags(int argc, char **argv, const char *usage) {
  gflags::SetUsageMessage(usage);  // Usage message is in rust.
  gflags::AllowCommandLineReparsing();

  // Command line arguments and help message.
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Initialize Google’s logging library.
  google::InitGoogleLogging("proxy");

  // Returned the read command line flags.
  return {FLAGS_workers, FLAGS_consistent, FLAGS_db_name.c_str(),
          FLAGS_hostname.c_str()};
}

// Initialize pelton_state in pelton.cc
bool FFIInitialize(size_t workers, bool consistent) {
  // call c++ function from C with converted types
  LOG(INFO) << "C-Wrapper: running pelton::initialize";
  if (pelton::initialize(workers, consistent)) {
    LOG(INFO) << "C-Wrapper: global connection opened";
    // Set signal handler.
    signal(SIGINT, proxy_terminate);
    signal(SIGTERM, proxy_terminate);
    return true;
  } else {
    LOG(FATAL) << "C-Wrapper: failed to open global connection";
    return false;
  }
}

// Open a connection for a single client. The returned struct has connected =
// true if successful. Otherwise connected = false
FFIConnection FFIOpen(const char *db_name) {
  // Create a new client connection.
  pelton::Connection *cpp_conn = new pelton::Connection();
  if (pelton::open(cpp_conn, db_name)) {
    return reinterpret_cast<FFIConnection>(cpp_conn);
  }
  LOG(FATAL) << "C-Wrapper: failed to open connection";
  return nullptr;
}

// Execute a DDL statement (e.g. CREATE TABLE, CREATE VIEW, CREATE INDEX).
bool FFIExecDDL(FFIConnection c_conn, const char *query) {
  pelton::Connection *cpp_conn = reinterpret_cast<pelton::Connection *>(c_conn);
  absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);
  if (result.ok()) {
    return result.value().Success();
  }
  LOG(FATAL) << "C-Wrapper: " << result.status();
  return false;
}

// Execute an update statement (e.g. INSERT, UPDATE, DELETE).
// Returns -1 if error, otherwise returns the number of affected rows.
FFIUpdateResult FFIExecUpdate(FFIConnection c_conn, const char *query) {
  pelton::Connection *cpp_conn = reinterpret_cast<pelton::Connection *>(c_conn);
  absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);
  if (result.ok()) {
    return {result.value().UpdateCount(), result.value().LastInsertId()};
  }
  LOG(FATAL) << "C-Wrapper: " << result.status();
  return {-1, 0};
}

// Executes a query (SELECT).
// Returns nullptr (0) on error.
FFIResult FFIExecSelect(FFIConnection c_conn, const char *query) {
  pelton::Connection *cpp_conn = reinterpret_cast<pelton::Connection *>(c_conn);

  // Execute query and get result.
  absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);
  if (result.ok()) {
    pelton::SqlResult *ptr = new pelton::SqlResult(std::move(result.value()));
    int *idx_ptr = new int();
    *idx_ptr = -1;
    return {reinterpret_cast<void *>(ptr), idx_ptr};
  }

  LOG(FATAL) << "C-Wrapper: " << result.status();
  return {nullptr, nullptr};
}

// Create a prepared statement.
FFIPreparedStatement FFIPrepare(FFIConnection c_conn, const char *query) {
  pelton::Connection *cpp_conn = reinterpret_cast<pelton::Connection *>(c_conn);
  absl::StatusOr<const pelton::PreparedStatement *> result =
      pelton::prepare(cpp_conn, query);
  if (result.ok()) {
    return reinterpret_cast<FFIPreparedStatement>(result.value());
  }
  LOG(FATAL) << "C-Wrapper: " << result.status();
  return nullptr;
}

// Execute a prepared statement.
FFIPreparedResult FFIExecPrepare(FFIConnection c_conn, size_t stmt_id,
                                 size_t arg_count, const char **arg_values) {
  pelton::Connection *cpp_conn = reinterpret_cast<pelton::Connection *>(c_conn);

  // Transform args to cpp format.
  std::vector<std::string> cpp_args;
  cpp_args.reserve(arg_count);
  for (size_t i = 0; i < arg_count; i++) {
    cpp_args.emplace_back(arg_values[i]);
  }

  // Call pelton.
  absl::StatusOr<pelton::SqlResult> result =
      pelton::exec(cpp_conn, stmt_id, cpp_args);
  if (result.ok()) {
    pelton::SqlResult &sql_result = result.value();
    if (sql_result.IsUpdate()) {
      int row_count = sql_result.UpdateCount();
      uint64_t lid = sql_result.LastInsertId();
      return {false, {nullptr, nullptr}, row_count, lid};
    } else if (sql_result.IsQuery()) {
      pelton::SqlResult *ptr = new pelton::SqlResult(std::move(result.value()));
      int *idx_ptr = new int();
      *idx_ptr = -1;
      return {true, {reinterpret_cast<void *>(ptr), idx_ptr}, -1, 0};
    } else {
      LOG(FATAL) << "Illegal prepared statement result type";
    }
  }

  LOG(FATAL) << "C-Wrapper: " << result.status();
  return {true, {nullptr, nullptr}, -1, 0};
}

// Close the connection. Returns true if successful and false otherwise.
bool FFIClose(FFIConnection c_conn) {
  pelton::Connection *cpp_conn = reinterpret_cast<pelton::Connection *>(c_conn);
  if (pelton::close(cpp_conn)) {
    delete cpp_conn;
    return true;
  } else {
    LOG(FATAL) << "C-Wrapper: failed to close connection";
    return false;
  }
}

// Shutdown the planner.
void FFIPlannerShutdown() { pelton::shutdown_planner(); }

// Delete pelton_state. Returns true if successful and false otherwise.
bool FFIShutdown() {
  LOG(INFO) << "C-Wrapper: Closing global connection";
  if (pelton::shutdown()) {
    LOG(INFO) << "C-Wrapper: global connection closed";
    return true;
  } else {
    LOG(FATAL) << "C-Wrapper: global connection close false";
    return false;
  }
}

// FFIResult schema handling.
bool FFIResultNextSet(FFIResult c_result) {
  (*c_result.index)++;
  int idx = *c_result.index;
  pelton::SqlResult *result =
      reinterpret_cast<pelton::SqlResult *>(c_result.result);
  if (idx >= 0 && static_cast<unsigned>(idx) < result->ResultSets().size()) {
    return true;
  } else {
    *c_result.index = -1;
    return false;
  }
}
size_t FFIResultColumnCount(FFIResult c_result) {
  int idx = *c_result.index;
  pelton::SqlResult *result =
      reinterpret_cast<pelton::SqlResult *>(c_result.result);
  const pelton::Schema &schema = result->ResultSets().at(idx).schema();
  return schema.size();
}
const char *FFIResultColumnName(FFIResult c_result, size_t col) {
  int idx = *c_result.index;
  pelton::SqlResult *result =
      reinterpret_cast<pelton::SqlResult *>(c_result.result);
  const pelton::Schema &schema = result->ResultSets().at(idx).schema();
  return schema.NameOf(col).c_str();
}
FFIColumnType FFIResultColumnType(FFIResult c_result, size_t col) {
  int idx = *c_result.index;
  pelton::SqlResult *result =
      reinterpret_cast<pelton::SqlResult *>(c_result.result);
  const pelton::Schema &schema = result->ResultSets().at(idx).schema();
  return static_cast<FFIColumnType>(schema.TypeOf(col));
}

// FFIResult data handling.
size_t FFIResultRowCount(FFIResult c_result) {
  int idx = *c_result.index;
  pelton::SqlResult *result =
      reinterpret_cast<pelton::SqlResult *>(c_result.result);
  const std::vector<pelton::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.size();
}
bool FFIResultIsNull(FFIResult c_result, size_t row, size_t col) {
  int idx = *c_result.index;
  pelton::SqlResult *result =
      reinterpret_cast<pelton::SqlResult *>(c_result.result);
  const std::vector<pelton::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.at(row).IsNull(col);
}
uint64_t FFIResultGetUInt(FFIResult c_result, size_t row, size_t col) {
  int idx = *c_result.index;
  pelton::SqlResult *result =
      reinterpret_cast<pelton::SqlResult *>(c_result.result);
  const std::vector<pelton::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.at(row).GetUInt(col);
}
int64_t FFIResultGetInt(FFIResult c_result, size_t row, size_t col) {
  int idx = *c_result.index;
  pelton::SqlResult *result =
      reinterpret_cast<pelton::SqlResult *>(c_result.result);
  const std::vector<pelton::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.at(row).GetInt(col);
}
const char *FFIResultGetString(FFIResult c_result, size_t row, size_t col) {
  int idx = *c_result.index;
  pelton::SqlResult *result =
      reinterpret_cast<pelton::SqlResult *>(c_result.result);
  const std::vector<pelton::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.at(row).GetString(col).c_str();
}
const char *FFIResultGetDatetime(FFIResult c_result, size_t row, size_t col) {
  int idx = *c_result.index;
  pelton::SqlResult *result =
      reinterpret_cast<pelton::SqlResult *>(c_result.result);
  const std::vector<pelton::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.at(row).GetDateTime(col).c_str();
}

// Clean up the memory allocated by an FFIResult.
void FFIResultDestroy(FFIResult c_result) {
  pelton::SqlResult *result =
      reinterpret_cast<pelton::SqlResult *>(c_result.result);
  delete result;
  delete c_result.index;
}

// FFIPreparedStatement handling.
size_t FFIPreparedStatementID(FFIPreparedStatement c_stmt) {
  const pelton::PreparedStatement *stmt =
      reinterpret_cast<const pelton::PreparedStatement *>(c_stmt);
  return stmt->stmt_id;
}
size_t FFIPreparedStatementArgCount(FFIPreparedStatement c_stmt) {
  const pelton::PreparedStatement *stmt =
      reinterpret_cast<const pelton::PreparedStatement *>(c_stmt);
  return stmt->total_count;
}
FFIColumnType FFIPreparedStatementArgType(FFIPreparedStatement c_stmt,
                                          size_t arg) {
  const pelton::PreparedStatement *stmt =
      reinterpret_cast<const pelton::PreparedStatement *>(c_stmt);
  size_t cumulative = 0;
  for (size_t i = 0; i < stmt->arg_value_count.size(); i++) {
    cumulative += stmt->arg_value_count.at(i);
    if (arg < cumulative) {
      return static_cast<FFIColumnType>(stmt->canonical->arg_types.at(i));
    }
  }
  LOG(FATAL) << "C-Wrapper: prepared argument beyond count";
  return FFIColumnType::INT;
}
