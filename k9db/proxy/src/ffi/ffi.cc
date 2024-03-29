#include "k9db/proxy/src/ffi/ffi.h"

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
#include "k9db/k9db.h"

DEFINE_uint32(workers, 1, "Number of workers");
DEFINE_bool(consistent, true, "Dataflow consistency with futures");
DEFINE_string(db_name, "k9db", "Name of the database");
DEFINE_string(hostname, "127.0.0.1:10001", "Hostname to bind against");
DEFINE_string(db_path, "", "Path to where to store db");

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
          FLAGS_hostname.c_str(), FLAGS_db_path.c_str()};
}

// Initialize k9db_state in k9db.cc
bool FFIInitialize(size_t workers, bool consistent, const char *db_name,
                   const char *db_path) {
  // call c++ function from C with converted types
  LOG(INFO) << "C-Wrapper: running k9db::initialize";
  if (k9db::initialize(workers, consistent, db_name, db_path)) {
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
FFIConnection FFIOpen() {
  // Create a new client connection.
  k9db::Connection *cpp_conn = new k9db::Connection();
  if (k9db::open(cpp_conn)) {
    return reinterpret_cast<FFIConnection>(cpp_conn);
  }
  LOG(FATAL) << "C-Wrapper: failed to open connection";
  return nullptr;
}

// Execute a DDL statement (e.g. CREATE TABLE, CREATE VIEW, CREATE INDEX).
bool FFIExecDDL(FFIConnection c_conn, const char *query) {
  k9db::Connection *cpp_conn = reinterpret_cast<k9db::Connection *>(c_conn);
  absl::StatusOr<k9db::SqlResult> result = k9db::exec(cpp_conn, query);
  if (result.ok()) {
    return result.value().Success();
  }
  LOG(FATAL) << "C-Wrapper: " << result.status();
  return false;
}

// Execute an update statement (e.g. INSERT, UPDATE, DELETE).
// Returns -1 if error, otherwise returns the number of affected rows.
FFIUpdateResult FFIExecUpdate(FFIConnection c_conn, const char *query) {
  k9db::Connection *cpp_conn = reinterpret_cast<k9db::Connection *>(c_conn);
  absl::StatusOr<k9db::SqlResult> result = k9db::exec(cpp_conn, query);
  if (result.ok()) {
    return {result.value().UpdateCount(), result.value().LastInsertId()};
  }
  LOG(FATAL) << "C-Wrapper: " << result.status();
  return {-1, 0};
}

// Executes a query (SELECT).
// Returns nullptr (0) on error.
FFIResult FFIExecSelect(FFIConnection c_conn, const char *query) {
  k9db::Connection *cpp_conn = reinterpret_cast<k9db::Connection *>(c_conn);

  // Execute query and get result.
  absl::StatusOr<k9db::SqlResult> result = k9db::exec(cpp_conn, query);
  if (result.ok()) {
    k9db::SqlResult *ptr = new k9db::SqlResult(std::move(result.value()));
    int *idx_ptr = new int();
    *idx_ptr = -1;
    return {reinterpret_cast<void *>(ptr), idx_ptr};
  }

  LOG(FATAL) << "C-Wrapper: " << result.status();
  return {nullptr, nullptr};
}

// Create a prepared statement.
FFIPreparedStatement FFIPrepare(FFIConnection c_conn, const char *query) {
  k9db::Connection *cpp_conn = reinterpret_cast<k9db::Connection *>(c_conn);
  absl::StatusOr<const k9db::PreparedStatement *> result =
      k9db::prepare(cpp_conn, query);
  if (result.ok()) {
    return reinterpret_cast<FFIPreparedStatement>(result.value());
  }
  LOG(FATAL) << "C-Wrapper: " << result.status();
  return nullptr;
}

// Execute a prepared statement.
FFIPreparedResult FFIExecPrepare(FFIConnection c_conn, size_t stmt_id,
                                 size_t arg_count, const char **arg_values) {
  k9db::Connection *cpp_conn = reinterpret_cast<k9db::Connection *>(c_conn);

  // Transform args to cpp format.
  std::vector<std::string> cpp_args;
  cpp_args.reserve(arg_count);
  for (size_t i = 0; i < arg_count; i++) {
    cpp_args.emplace_back(arg_values[i]);
  }

  // Call k9db.
  absl::StatusOr<k9db::SqlResult> result =
      k9db::exec(cpp_conn, stmt_id, cpp_args);
  if (result.ok()) {
    k9db::SqlResult &sql_result = result.value();
    if (sql_result.IsUpdate()) {
      int row_count = sql_result.UpdateCount();
      uint64_t lid = sql_result.LastInsertId();
      return {false, {nullptr, nullptr}, row_count, lid};
    } else if (sql_result.IsQuery()) {
      k9db::SqlResult *ptr = new k9db::SqlResult(std::move(result.value()));
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
  k9db::Connection *cpp_conn = reinterpret_cast<k9db::Connection *>(c_conn);
  if (k9db::close(cpp_conn)) {
    delete cpp_conn;
    return true;
  } else {
    LOG(FATAL) << "C-Wrapper: failed to close connection";
    return false;
  }
}

// Shutdown the planner.
void FFIPlannerShutdown() { k9db::shutdown_planner(); }

// Delete k9db_state. Returns true if successful and false otherwise.
bool FFIShutdown() {
  LOG(INFO) << "C-Wrapper: Closing global connection";
  if (k9db::shutdown()) {
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
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  if (idx >= 0 && static_cast<unsigned>(idx) < result->ResultSets().size()) {
    return true;
  } else {
    *c_result.index = -1;
    return false;
  }
}
size_t FFIResultColumnCount(FFIResult c_result) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const k9db::Schema &schema = result->ResultSets().at(idx).schema();
  return schema.size();
}
const char *FFIResultColumnName(FFIResult c_result, size_t col) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const k9db::Schema &schema = result->ResultSets().at(idx).schema();
  return schema.NameOf(col).c_str();
}
FFIColumnType FFIResultColumnType(FFIResult c_result, size_t col) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const k9db::Schema &schema = result->ResultSets().at(idx).schema();
  return static_cast<FFIColumnType>(schema.TypeOf(col));
}

// FFIResult data handling.
size_t FFIResultRowCount(FFIResult c_result) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const std::vector<k9db::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.size();
}
bool FFIResultIsNull(FFIResult c_result, size_t row, size_t col) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const std::vector<k9db::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.at(row).IsNull(col);
}
uint64_t FFIResultGetUInt(FFIResult c_result, size_t row, size_t col) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const std::vector<k9db::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.at(row).GetUInt(col);
}
int64_t FFIResultGetInt(FFIResult c_result, size_t row, size_t col) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const std::vector<k9db::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.at(row).GetInt(col);
}
const char *FFIResultGetString(FFIResult c_result, size_t row, size_t col) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const std::vector<k9db::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.at(row).GetString(col).c_str();
}
const char *FFIResultGetDatetime(FFIResult c_result, size_t row, size_t col) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const std::vector<k9db::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.at(row).GetDateTime(col).c_str();
}

// Clean up the memory allocated by an FFIResult.
void FFIResultDestroy(FFIResult c_result) {
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  delete result;
  delete c_result.index;
}

// FFIPreparedStatement handling.
size_t FFIPreparedStatementID(FFIPreparedStatement c_stmt) {
  const k9db::PreparedStatement *stmt =
      reinterpret_cast<const k9db::PreparedStatement *>(c_stmt);
  return stmt->stmt_id;
}
size_t FFIPreparedStatementArgCount(FFIPreparedStatement c_stmt) {
  const k9db::PreparedStatement *stmt =
      reinterpret_cast<const k9db::PreparedStatement *>(c_stmt);
  return stmt->total_count;
}
FFIColumnType FFIPreparedStatementArgType(FFIPreparedStatement c_stmt,
                                          size_t arg) {
  const k9db::PreparedStatement *stmt =
      reinterpret_cast<const k9db::PreparedStatement *>(c_stmt);
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
