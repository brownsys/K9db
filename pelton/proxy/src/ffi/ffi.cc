#include "pelton/proxy/src/ffi/ffi.h"

#include <stdlib.h>
#include <string.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "pelton/pelton.h"

DEFINE_uint32(workers, 3, "Number of workers");
DEFINE_bool(consistent, true, "Dataflow consistency with futures");
DEFINE_string(db_name, "pelton", "Name of the database");

// process command line arguments with gflags
FFIArgs FFIGflags(int argc, char **argv, const char *usage) {
  gflags::SetUsageMessage(usage);  // Usage message is in rust.
  gflags::AllowCommandLineReparsing();

  // Command line arguments and help message.
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Initialize Google’s logging library.
  google::InitGoogleLogging("proxy");

  // Returned the read command line flags.
  return {FLAGS_workers, FLAGS_consistent, FLAGS_db_name.c_str()};
}

// Initialize pelton_state in pelton.cc
bool FFIInitialize(size_t workers, bool consistent) {
  // call c++ function from C with converted types
  LOG(INFO) << "C-Wrapper: running pelton::initialize";
  if (pelton::initialize(workers, consistent)) {
    LOG(INFO) << "C-Wrapper: global connection opened";
  } else {
    LOG(FATAL) << "C-Wrapper: failed to open global connection";
    return false;
  }
  return true;
}

// Open a connection for a single client. The returned struct has connected =
// true if successful. Otherwise connected = false
FFIConnection FFIOpen(const char *db_name) {
  // convert char* to const std::string
  const std::string c_db(db_name);

  // Create a new client connection.
  FFIConnection c_conn = {new pelton::Connection(), false};

  // convert void pointer of ConnectionC struct into c++ class instance
  // Connection
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn.cpp_conn);

  // call c++ function from C with converted types
  if (pelton::open(cpp_conn, c_db)) {
    c_conn.connected = true;
  } else {
    LOG(FATAL) << "C-Wrapper: failed to open connection";
    c_conn.connected = false;
  }
  return c_conn;
}

// Delete pelton_state. Returns true if successful and false otherwise.
bool FFIShutdown() {
  LOG(INFO) << "C-Wrapper: Closing global connection";
  bool response = pelton::shutdown();
  if (response) {
    LOG(INFO) << "C-Wrapper: global connection closed";
    return true;
  } else {
    LOG(FATAL) << "C-Wrapper: global connection close false";
    return false;
  }
}

// Close the connection. Returns true if successful and false otherwise.
bool FFIClose(FFIConnection *c_conn) {
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);

  if (cpp_conn != nullptr) {
    bool response = pelton::close(cpp_conn);
    if (response) {
      delete reinterpret_cast<pelton::Connection *>(cpp_conn);
      c_conn->connected = false;
      c_conn->cpp_conn = nullptr;
      return true;
    } else {
      LOG(FATAL) << "C-Wrapper: failed to close connection";
      return false;
    }
  }
  return true;
}

// Execute a DDL statement (e.g. CREATE TABLE, CREATE VIEW, CREATE INDEX).
bool FFIExecDDL(FFIConnection *c_conn, const char *query) {
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
  absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);
  if (!result.ok()) {
    LOG(FATAL) << "C-Wrapper: " << result.status();
  }
  return result.ok() && result.value().Success();
}

// Execute an update statement (e.g. INSERT, UPDATE, DELETE).
// Returns -1 if error, otherwise returns the number of affected rows.
int FFIExecUpdate(FFIConnection *c_conn, const char *query) {
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
  absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);
  if (!result.ok()) {
    LOG(FATAL) << "C-Wrapper: " << result.status();
  }
  if (result.ok()) {
    return result.value().UpdateCount();
  } else {
    return -1;
  }
}

// Helper functions for handling queries (SELECTs).
namespace {

using CType = pelton::sqlast::ColumnDefinition::Type;

void PopulateSchema(FFIResult *c_result, const pelton::Schema &schema) {
  // for each column
  for (size_t i = 0; i < c_result->num_cols; i++) {
    const std::string &cpp_col_name = schema.NameOf(i);
    // Cannot use malloc here: rust will eventually have ownership of this
    // string, and it cleans it up using free, not delete.
    c_result->col_names[i] =
        static_cast<char *>(malloc(cpp_col_name.size() + 1));
    memcpy(c_result->col_names[i], cpp_col_name.c_str(),
           cpp_col_name.size() + 1);

    // set column type
    CType col_type = schema.TypeOf(i);
    switch (col_type) {
      case CType::INT:
        c_result->col_types[i] = FFIColumnType::INT;
        break;
      case CType::UINT:
        c_result->col_types[i] = FFIColumnType::UINT;
        break;
      case CType::TEXT:
        c_result->col_types[i] = FFIColumnType::TEXT;
        break;
      case CType::DATETIME:
        c_result->col_types[i] = FFIColumnType::DATETIME;
        break;
      default:
        LOG(FATAL) << "C-Wrapper: Unrecognizable column type " << col_type;
    }
  }
}

void PopulateRecords(FFIResult *c_result,
                     const std::vector<pelton::Record> &records) {
  size_t num_rows = c_result->num_rows;
  size_t num_cols = c_result->num_cols;
  // for every record (row)
  for (size_t i = 0; i < num_rows; i++) {
    // for every col in this row
    for (size_t j = 0; j < num_cols; j++) {
      // current row index * num cols per row + current col index
      size_t index = i * num_cols + j;
      if (records[i].IsNull(j)) {
        c_result->records[index].is_null = true;
      } else {
        c_result->records[index].is_null = false;
        switch (c_result->col_types[j]) {
          case FFIColumnType::UINT:
            c_result->records[index].record.UINT = records[i].GetUInt(j);
            break;
          case FFIColumnType::INT:
            c_result->records[index].record.INT = records[i].GetInt(j);
            break;
          case FFIColumnType::TEXT: {
            const std::string &cpp_val = records[i].GetString(j);
            c_result->records[index].record.TEXT =
                static_cast<char *>(malloc(cpp_val.size() + 1));
            memcpy(c_result->records[index].record.TEXT, cpp_val.c_str(),
                   cpp_val.size() + 1);
            break;
          }
          case FFIColumnType::DATETIME: {
            const std::string &cpp_val = records[i].GetDateTime(j);
            c_result->records[index].record.DATETIME =
                static_cast<char *>(malloc(cpp_val.size() + 1));
            memcpy(c_result->records[index].record.DATETIME, cpp_val.c_str(),
                   cpp_val.size() + 1);
            break;
          }
          default:
            LOG(FATAL) << "C-Wrapper: Invalid col_type: "
                       << c_result->col_types[j];
        }
      }
    }
  }
}

}  // namespace

// Executes a query (SELECT).
// Returns nullptr (0) on error.
FFIResult *FFIExecSelect(FFIConnection *c_conn, const char *query) {
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
  absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);
  if (!result.ok()) {
    LOG(FATAL) << "C-Wrapper: " << result.status();
  }

  if (result.ok()) {
    pelton::SqlResult &sql_result = result.value();
    for (pelton::SqlResultSet &resultset : sql_result.ResultSets()) {
      std::vector<pelton::dataflow::Record> records = resultset.Vec();
      size_t num_rows = records.size();
      size_t num_cols = resultset.schema().size();

      // allocate memory for CResult struct and the flexible array of RecordData
      // unions
      // we have to use malloc here to account for the flexible array.
      FFIResult *c_result = static_cast<FFIResult *>(
          malloc(sizeof(FFIResult) + sizeof(FFIRecord) * num_rows * num_cols));
      //  |- FFIResult*-|   |struct FFIRecord| |num of records (rows) and cols |

      // set number of columns
      c_result->num_cols = num_cols;
      c_result->num_rows = num_rows;
      PopulateSchema(c_result, resultset.schema());
      PopulateRecords(c_result, records);
      return c_result;  // TODO(babman): this is where we can support GDPR GET.
    }
  }
  return nullptr;
}

// Clean up the memory allocated by an FFIResult.
void FFIDestroySelect(FFIResult *c_result) { free(c_result); }

void FFIPlannerShutdown() { pelton::shutdown_planner(); }

// Create a prepared statement.
FFIPreparedStatement *FFIPrepare(FFIConnection *c_conn, const char *query) {
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
  absl::StatusOr<const pelton::PreparedStatementDescriptor *> result =
      pelton::prepare(cpp_conn, query);
  if (!result.ok()) {
    LOG(FATAL) << "C-Wrapper: " << result.status();
  }

  if (result.ok()) {
    const auto *descriptor = result.value();
    size_t len = descriptor->total_count;
    // Allocate FFIPreparedStatement with the right number of types.
    FFIPreparedStatement *c_result = static_cast<FFIPreparedStatement *>(
        malloc(sizeof(FFIPreparedStatement) + sizeof(FFIColumnType) * len));
    // Fill in FFIPreparedStatement.
    c_result->stmt_id = descriptor->stmt_id;
    c_result->arg_count = len;
    size_t current = 0;
    for (size_t i = 0; i < descriptor->arg_flow_key.size(); i++) {
      size_t key_index = descriptor->arg_flow_key.at(i);
      size_t value_count = descriptor->arg_value_count.at(i);
      FFIColumnType c_type;
      auto type = descriptor->flow_descriptor->key_types.at(key_index);
      switch (type) {
        case CType::INT:
          c_type = FFIColumnType::INT;
          break;
        case CType::UINT:
          c_type = FFIColumnType::UINT;
          break;
        case CType::TEXT:
          c_type = FFIColumnType::TEXT;
          break;
        case CType::DATETIME:
          c_type = FFIColumnType::DATETIME;
          break;
        default:
          LOG(FATAL) << "C-Wrapper: Unrecognizable column type " << type;
      }
      for (size_t j = 0; j < value_count; j++) {
        c_result->args[current + j] = c_type;
      }
      current += value_count;
    }
    return c_result;
  }
  return nullptr;
}
void FFIDestroyPreparedStatement(FFIPreparedStatement *c_stmt) { free(c_stmt); }

// Execute a prepared statement.
FFIResult *FFIExecPrepare(FFIConnection *c_conn, size_t stmt_id,
                          size_t arg_count, const char **arg_values) {
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
  // Transform args to cpp format.
  std::vector<std::string> cpp_args;
  cpp_args.reserve(arg_count);
  for (size_t i = 0; i < arg_count; i++) {
    cpp_args.emplace_back(arg_values[i]);
  }
  // Call pelton.
  absl::StatusOr<pelton::SqlResult> result =
      pelton::exec(cpp_conn, stmt_id, cpp_args);
  if (!result.ok()) {
    LOG(FATAL) << "C-Wrapper: " << result.status();
  }

  if (result.ok()) {
    pelton::SqlResult &sql_result = result.value();
    for (pelton::SqlResultSet &resultset : sql_result.ResultSets()) {
      std::vector<pelton::dataflow::Record> records = resultset.Vec();
      size_t num_rows = records.size();
      size_t num_cols = resultset.schema().size();

      // allocate memory for CResult struct and the flexible array of RecordData
      // unions
      // we have to use malloc here to account for the flexible array.
      FFIResult *c_result = static_cast<FFIResult *>(
          malloc(sizeof(FFIResult) + sizeof(FFIRecord) * num_rows * num_cols));
      //  |- FFIResult*-|   |struct FFIRecord| |num of records (rows) and cols |

      // set number of columns
      c_result->num_cols = num_cols;
      c_result->num_rows = num_rows;
      PopulateSchema(c_result, resultset.schema());
      PopulateRecords(c_result, records);
      return c_result;
    }
  }
  return nullptr;
}
