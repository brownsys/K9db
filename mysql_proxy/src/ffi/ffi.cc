#include "mysql_proxy/src/ffi/ffi.h"

#include <stdlib.h>
#include <string.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "glog/logging.h"
#include "pelton/pelton.h"

// Open a connection. The returned struct has connected = true if successful.
// Otherwise connected = false.
FFIConnection FFIOpen(const char *db_dir, const char *db_username,
                      const char *db_password) {
  // Log debuggin information.
  LOG(INFO) << "C-Wrapper: starting open_c";
  LOG(INFO) << "C-Wrapper: db_dir is: " << std::string(db_dir);
  LOG(INFO) << "C-Wrapper: db_username is: " << std::string(db_username);
  LOG(INFO) << "C-Wrapper: db_passwored is: " << std::string(db_password);

  // Create a new connection.
  FFIConnection c_conn = {new pelton::Connection(), false};

  // convert void pointer of ConnectionC struct into c++ class instance
  // Connection
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn.cpp_conn);

  // convert char* to const std::string
  const std::string c_db_dir(db_dir);
  const std::string c_db_username(db_username);
  const std::string c_db_password(db_password);

  // call c++ function from C with converted types
  LOG(INFO) << "C-Wrapper: running pelton::open";
  if (pelton::open(c_db_dir, c_db_username, c_db_password, cpp_conn)) {
    LOG(INFO) << "C-Wrapper: connection opened";
    c_conn.connected = true;
  } else {
    LOG(INFO) << "C-Wrapper: failed to open connection";
    c_conn.connected = false;
  }

  return c_conn;
}

// Close the connection. Returns true if successful and false otherwise.
bool FFIClose(FFIConnection *c_conn) {
  LOG(INFO) << "C-Wrapper: starting close_c";
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);

  if (cpp_conn != nullptr) {
    bool response = pelton::close(cpp_conn);
    if (response) {
      LOG(INFO) << "C-Wrapper: connection closed";
      delete reinterpret_cast<pelton::Connection *>(cpp_conn);
      c_conn->connected = false;
      c_conn->cpp_conn = nullptr;
      return true;
    } else {
      LOG(INFO) << "C-Wrapper: failed to close connection";
      return false;
    }
  }
  return true;
}

// Execute a DDL statement (e.g. CREATE TABLE, CREATE VIEW, CREATE INDEX).
bool FFIExecDDL(FFIConnection *c_conn, const char *query) {
  LOG(INFO) << "C-Wrapper: executing ddl " << std::string(query);
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
  absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);
  if (!result.ok()) {
    LOG(INFO) << "C-Wrapper: " << result.status();
  }
  return result.ok() && result.value().IsStatement() &&
         result.value().Success();
}

// Execute an update statement (e.g. INSERT, UPDATE, DELETE).
// Returns -1 if error, otherwise returns the number of affected rows.
int FFIExecUpdate(FFIConnection *c_conn, const char *query) {
  LOG(INFO) << "C-Wrapper: executing update " << std::string(query);
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
  absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);
  if (!result.ok()) {
    LOG(INFO) << "C-Wrapper: " << result.status();
  }
  if (result.ok() && result.value().IsUpdate()) {
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
        LOG(INFO) << "C-Wrapper: Unrecognizable column type " << col_type;
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
      switch (c_result->col_types[j]) {
        case FFIColumnType::UINT:
          c_result->values[index].UINT = records[i].GetUInt(j);
          break;
        case FFIColumnType::INT:
          c_result->values[index].INT = records[i].GetInt(j);
          break;
        case FFIColumnType::TEXT: {
          const std::string &cpp_val = records[i].GetString(j);
          c_result->values[index].TEXT =
              static_cast<char *>(malloc(cpp_val.size() + 1));
          memcpy(c_result->values[index].TEXT, cpp_val.c_str(),
                 cpp_val.size() + 1);
          break;
        }
        case FFIColumnType::DATETIME: {
          const std::string &cpp_val = records[i].GetDateTime(j);
          c_result->values[index].DATETIME =
              static_cast<char *>(malloc(cpp_val.size() + 1));
          memcpy(c_result->values[index].DATETIME, cpp_val.c_str(),
                 cpp_val.size() + 1);
        }
        default:
          LOG(FATAL) << "C-Wrapper: Invalid col_type";
      }
    }
  }
}

}  // namespace

// Executes a query (SELECT).
// Returns nullptr (0) on error.
FFIResult *FFIExecSelect(FFIConnection *c_conn, const char *query) {
  LOG(INFO) << "C-Wrapper: executing query " << std::string(query);
  pelton::Connection *cpp_conn =
      reinterpret_cast<pelton::Connection *>(c_conn->cpp_conn);
  absl::StatusOr<pelton::SqlResult> result = pelton::exec(cpp_conn, query);
  if (!result.ok()) {
    LOG(INFO) << "C-Wrapper: " << result.status();
  }

  if (result.ok()) {
    pelton::SqlResult &sql_result = result.value();
    if (sql_result.IsQuery() && sql_result.HasResultSet()) {
      std::unique_ptr<pelton::SqlResultSet> resultset =
          sql_result.NextResultSet();
      std::vector<pelton::dataflow::Record> records = resultset->Vectorize();
      size_t num_rows = records.size();
      size_t num_cols = resultset->GetSchema().size();

      LOG(INFO) << "C-Wrapper: malloc FFIResult";
      // allocate memory for CResult struct and the flexible array of RecordData
      // unions
      // we have to use malloc here to account for the flexible array.
      FFIResult *c_result = static_cast<FFIResult *>(
          malloc(sizeof(FFIResult) +
                 sizeof(FFIResult::RecordData) * num_rows * num_cols));
      //  |- FFIResult*-|   |union RecordData| |num of records (rows) and cols |

      // set number of columns
      c_result->num_cols = num_cols;
      c_result->num_rows = num_rows;
      PopulateSchema(c_result, resultset->GetSchema());
      PopulateRecords(c_result, records);
      return c_result;
    }
  }

  LOG(INFO) << "C-Wrapper: Result.ok() or result.value().IsQuery() failed";
  return nullptr;
}

// Clean up the memory allocated by an FFIResult.
void FFIDestroySelect(FFIResult *c_result) {
  LOG(INFO) << "C-Wrapper: starting destroy_select to delete CResult";
  free(c_result);
}
