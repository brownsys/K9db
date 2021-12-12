#include "mysql_proxy/src/ffi/ffi.h"

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
  VLOG(2) << "C-Wrapper: running pelton::initialize";
  if (pelton::initialize(workers, consistent)) {
    VLOG(1) << "C-Wrapper: global connection opened";
  } else {
    LOG(ERROR) << "C-Wrapper: failed to open global connection";
    return false;
  }
  return true;
}

// Open a connection for a single client. The returned struct has connected =
// true if successful. Otherwise connected = false
FFIConnection FFIOpen(const char *db_name) {
  // Log debugging information
  VLOG(2) << "C-Wrapper: db_name is: " << std::string(db_name);

  // convert char* to const std::string
  const std::string c_db(db_name);

  // Create a new client connection.
  FFIConnection c_conn = {new pelton::Connection(), false};

  // convert void pointer of ConnectionC struct into c++ class instance
  // Connection
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn.cpp_conn);

  // call c++ function from C with converted types
  VLOG(2) << "C-Wrapper: running pelton::open";
  if (pelton::open(cpp_conn, c_db)) {
    VLOG(1) << "C-Wrapper: connection opened";
    c_conn.connected = true;
  } else {
    LOG(ERROR) << "C-Wrapper: failed to open connection";
    c_conn.connected = false;
  }
  return c_conn;
}

// Delete pelton_state. Returns true if successful and false otherwise.
bool FFIShutdown() {
  VLOG(2) << "C-Wrapper: Closing global connection";
  bool response = pelton::shutdown();
  if (response) {
    VLOG(1) << "C-Wrapper: global connection closed";
    return true;
  }
  return false;
}

// Close the connection. Returns true if successful and false otherwise.
bool FFIClose(FFIConnection *c_conn) {
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);

  if (cpp_conn != nullptr) {
    bool response = pelton::close(cpp_conn);
    if (response) {
      VLOG(1) << "C-Wrapper: connection closed";
      delete reinterpret_cast<pelton::Connection *>(cpp_conn);
      c_conn->connected = false;
      c_conn->cpp_conn = nullptr;
      return true;
    } else {
      LOG(ERROR) << "C-Wrapper: failed to close connection";
      return false;
    }
  }
  return true;
}

// Execute a DDL statement (e.g. CREATE TABLE, CREATE VIEW, CREATE INDEX).
bool FFIExecDDL(FFIConnection *c_conn, const char *query) {
  VLOG(2) << "C-Wrapper: executing ddl " << std::string(query);
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
  absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);
  if (!result.ok()) {
    VLOG(2) << "C-Wrapper: " << result.status();
  }
  return result.ok() && result.value().Success();
}

// Execute an update statement (e.g. INSERT, UPDATE, DELETE).
// Returns -1 if error, otherwise returns the number of affected rows.
int FFIExecUpdate(FFIConnection *c_conn, const char *query) {
  VLOG(2) << "C-Wrapper: executing update " << std::string(query);
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
  absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);
  if (!result.ok()) {
    VLOG(2) << "C-Wrapper: " << result.status();
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
  VLOG(2) << "C-Wrapper: executing query " << std::string(query);
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
  absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);
  if (!result.ok()) {
    VLOG(2) << "C-Wrapper: " << result.status();
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
  } else {
    VLOG(2) << "C-Wrapper: Result.ok()" << result.status();
  }
  return nullptr;
}

// Clean up the memory allocated by an FFIResult.
void FFIDestroySelect(FFIResult *c_result) { free(c_result); }
