#include "k9db/proxy/src/ffi/ffi.h"

#include <signal.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
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

// Macros and helpers for creating Error wrapped results quickly.
namespace {

// Make a result with no error.
#define Ok(...)                 \
  {                             \
    __VA_ARGS__, { 0, nullptr } \
  }

// Turn c++ into an owned c-style string (the rust code will free it using
// FFIFreeError later).
char *to_c_string(const std::string &error) {
  char *result = new char[error.size() + 1];
  std::copy(error.begin(), error.end(), result);
  result[error.size()] = '\0';
  return result;
}
uint16_t code_to_uint(k9db::Error::Code code) {
  return static_cast<uint16_t>(code);
}

// Make a result with an error and undefined result.
#define Err(code, msg)                           \
  {                                              \
    {}, { code_to_uint(code), to_c_string(msg) } \
  }

}  // namespace

// Free error after use in rust.
void FFIFreeError(FFIError error) {
  if (error.message != nullptr) {
    delete[] error.message;
  }
}

// process command line arguments with gflags
FFIArgsOrError FFIGflags(int argc, char **argv, const char *usage) {
  gflags::SetUsageMessage(usage);  // Usage message is in rust.
  gflags::AllowCommandLineReparsing();

  // Command line arguments and help message.
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Initialize Google’s logging library.
  google::InitGoogleLogging("proxy");

  // Returned the read command line flags.
  return Ok({FLAGS_workers, FLAGS_consistent, FLAGS_db_name.c_str(),
             FLAGS_hostname.c_str(), FLAGS_db_path.c_str()});
}

// Initialize k9db_state in k9db.cc
FFISuccessOrError FFIInitialize(size_t workers, bool consistent,
                                const char *db_name, const char *db_path) {
  // call c++ function from C with converted types
  LOG(INFO) << "C-Wrapper: running k9db::initialize";
  if (k9db::initialize(workers, consistent, db_name, db_path)) {
    LOG(INFO) << "C-Wrapper: global connection opened";
    // Set signal handler.
    signal(SIGINT, proxy_terminate);
    signal(SIGTERM, proxy_terminate);
    return Ok(true);
  } else {
    LOG(INFO) << "C-Wrapper: failed to open global connection";
    return Ok(false);
  }
}

// Open a connection for a single client. The returned struct has connected =
// true if successful. Otherwise connected = false
FFIConnectionOrError FFIOpen() {
  // Create a new client connection.
  k9db::Connection *cpp_conn = new k9db::Connection();
  if (k9db::open(cpp_conn)) {
    return Ok(reinterpret_cast<FFIConnection>(cpp_conn));
  }
  LOG(WARNING) << "C-Wrapper: failed to open connection";
  return Err(k9db::Error::Code::ER_INTERNAL_ERROR, "Failed to open connection");
}

// Execute a DDL statement (e.g. CREATE TABLE, CREATE VIEW, CREATE INDEX).
FFISuccessOrError FFIExecDDL(FFIConnection c_conn, const char *query) {
  k9db::Connection *cpp_conn = reinterpret_cast<k9db::Connection *>(c_conn);
  absl::StatusOr<k9db::SqlResult> result = k9db::exec(cpp_conn, query);
  if (result.ok()) {
    return Ok(result.value().Success());
  }
  LOG(WARNING) << "C-Wrapper: " << result.status();
  return Err(k9db::Error::Code::ER_INTERNAL_ERROR, result.status().ToString());
}

// Execute an update statement (e.g. INSERT, UPDATE, DELETE).
// Returns -1 if error, otherwise returns the number of affected rows.
FFIUpdateResultOrError FFIExecUpdate(FFIConnection c_conn, const char *query) {
  k9db::Connection *cpp_conn = reinterpret_cast<k9db::Connection *>(c_conn);
  absl::StatusOr<k9db::SqlResult> result = k9db::exec(cpp_conn, query);
  if (result.ok()) {
    return Ok({result.value().UpdateCount(), result.value().LastInsertId()});
  }
  LOG(WARNING) << "C-Wrapper: " << result.status();
  return Err(k9db::Error::Code::ER_INTERNAL_ERROR, result.status().ToString());
}

// Executes a query (SELECT).
// Returns nullptr (0) on error.
FFIQueryResultOrError FFIExecSelect(FFIConnection c_conn, const char *query) {
  k9db::Connection *cpp_conn = reinterpret_cast<k9db::Connection *>(c_conn);

  // Execute query and get result.
  absl::StatusOr<k9db::SqlResult> result = k9db::exec(cpp_conn, query);
  if (result.ok()) {
    k9db::SqlResult *ptr = new k9db::SqlResult(std::move(result.value()));
    int *idx_ptr = new int();
    *idx_ptr = -1;
    return Ok({reinterpret_cast<void *>(ptr), idx_ptr});
  }

  LOG(WARNING) << "C-Wrapper: " << result.status();
  return Err(k9db::Error::Code::ER_INTERNAL_ERROR, result.status().ToString());
}

// Create a prepared statement.
FFIPreparedStatementOrError FFIPrepare(FFIConnection c_conn,
                                       const char *query) {
  k9db::Connection *cpp_conn = reinterpret_cast<k9db::Connection *>(c_conn);
  absl::StatusOr<const k9db::PreparedStatement *> result =
      k9db::prepare(cpp_conn, query);
  if (result.ok()) {
    return Ok(reinterpret_cast<FFIPreparedStatement>(result.value()));
  }
  LOG(WARNING) << "C-Wrapper: " << result.status();
  return Err(k9db::Error::Code::ER_INTERNAL_ERROR, result.status().ToString());
}

// Execute a prepared statement.
FFIPreparedResultOrError FFIExecPrepare(FFIConnection c_conn, size_t stmt_id,
                                        size_t arg_count,
                                        const char **arg_values) {
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
    FFIPreparedResult output = {};
    if (sql_result.IsUpdate()) {
      int row_count = sql_result.UpdateCount();
      uint64_t lid = sql_result.LastInsertId();
      output.query = false;
      output.update_result = {row_count, lid};
    } else if (sql_result.IsQuery()) {
      k9db::SqlResult *ptr = new k9db::SqlResult(std::move(result.value()));
      int *idx_ptr = new int();
      *idx_ptr = -1;
      output.query = true;
      output.query_result = {reinterpret_cast<void *>(ptr), idx_ptr};
    } else {
      LOG(WARNING) << "Illegal prepared statement result type";
      return Err(k9db::Error::Code::ER_INTERNAL_ERROR,
                 "Illegal prepared statement result type");
    }
    return Ok(output);
  }

  LOG(WARNING) << "C-Wrapper: " << result.status();
  return Err(k9db::Error::Code::ER_INTERNAL_ERROR, result.status().ToString());
}

// Close the connection. Returns true if successful and false otherwise.
FFISuccessOrError FFIClose(FFIConnection c_conn) {
  k9db::Connection *cpp_conn = reinterpret_cast<k9db::Connection *>(c_conn);
  if (k9db::close(cpp_conn)) {
    delete cpp_conn;
    return Ok(true);
  } else {
    LOG(WARNING) << "C-Wrapper: failed to close connection";
    return Ok(false);
  }
}

// Shutdown the planner.
void FFIPlannerShutdown() { k9db::shutdown_planner(); }

// Delete k9db_state. Returns true if successful and false otherwise.
FFISuccessOrError FFIShutdown() {
  LOG(INFO) << "C-Wrapper: Closing global connection";
  if (k9db::shutdown()) {
    LOG(INFO) << "C-Wrapper: global connection closed";
    return Ok(true);
  } else {
    LOG(WARNING) << "C-Wrapper: global connection close false";
    return Ok(false);
  }
}

// FFIResult schema handling.
bool FFIResultNextSet(FFIQueryResult c_result) {
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
size_t FFIResultColumnCount(FFIQueryResult c_result) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const k9db::Schema &schema = result->ResultSets().at(idx).schema();
  return schema.size();
}
const char *FFIResultTableName(FFIQueryResult c_result) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const std::string &table_name = result->ResultSets().at(idx).table_name();
  return table_name.c_str();
}
const char *FFIResultColumnName(FFIQueryResult c_result, size_t col) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const k9db::Schema &schema = result->ResultSets().at(idx).schema();
  return schema.NameOf(col).c_str();
}
FFIColumnType FFIResultColumnType(FFIQueryResult c_result, size_t col) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const k9db::Schema &schema = result->ResultSets().at(idx).schema();
  return static_cast<FFIColumnType>(schema.TypeOf(col));
}

// FFIResult data handling.
size_t FFIResultRowCount(FFIQueryResult c_result) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const std::vector<k9db::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.size();
}
bool FFIResultIsNull(FFIQueryResult c_result, size_t row, size_t col) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const std::vector<k9db::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.at(row).IsNull(col);
}
uint64_t FFIResultGetUInt(FFIQueryResult c_result, size_t row, size_t col) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const std::vector<k9db::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.at(row).GetUInt(col);
}
int64_t FFIResultGetInt(FFIQueryResult c_result, size_t row, size_t col) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const std::vector<k9db::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.at(row).GetInt(col);
}
const char *FFIResultGetString(FFIQueryResult c_result, size_t row,
                               size_t col) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const std::vector<k9db::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.at(row).GetString(col).c_str();
}
const char *FFIResultGetDatetime(FFIQueryResult c_result, size_t row,
                                 size_t col) {
  int idx = *c_result.index;
  k9db::SqlResult *result =
      reinterpret_cast<k9db::SqlResult *>(c_result.result);
  const std::vector<k9db::Record> &rows = result->ResultSets().at(idx).rows();
  return rows.at(row).GetDateTime(col).c_str();
}

// Clean up the memory allocated by an FFIResult.
void FFIResultDestroy(FFIQueryResult c_result) {
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
FFIColumnTypeOrError FFIPreparedStatementArgType(FFIPreparedStatement c_stmt,
                                                 size_t arg) {
  const k9db::PreparedStatement *stmt =
      reinterpret_cast<const k9db::PreparedStatement *>(c_stmt);
  size_t cumulative = 0;
  for (size_t i = 0; i < stmt->arg_value_count.size(); i++) {
    cumulative += stmt->arg_value_count.at(i);
    if (arg < cumulative) {
      return Ok(static_cast<FFIColumnType>(stmt->canonical->arg_types.at(i)));
    }
  }
  LOG(WARNING) << "C-Wrapper: prepared argument beyond count";
  return Err(k9db::Error::Code::ER_INTERNAL_ERROR,
             "prepared argument beyond parameter count");
}
