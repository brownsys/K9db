#ifndef PELTON_PROXY_SRC_FFI_FFI_H_
#define PELTON_PROXY_SRC_FFI_FFI_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

// Connection is essentially a wrapper around pelton connection.
typedef struct {
  // cpp_conn is in reality of type pelton::ConnectionLocal.
  void *cpp_conn;
  // connected is used to report whether some error was encountered when opening
  // the connection.
  bool connected;
} FFIConnection;

typedef struct {
  size_t workers;
  bool consistent;
  const char *db_name;
} FFIArgs;

// Our version of pelton/sqlast/ast_schema_enums.h#ColumnDefinitionTypeEnum.
typedef enum { UINT, INT, TEXT, DATETIME } FFIColumnType;

// Our representation of a Query result record (row)
typedef struct {
  bool is_null;
  union RecordData {
    uint64_t UINT;
    int64_t INT;
    char *TEXT;
    char *DATETIME;
  } record;
} FFIRecord;

// Our representation of a Query result. Contains schema information as well
// as the result data.
// Data inside FFIResult is owned by the FFIResult and must be freed manually
// when FFIResult expires.
typedef struct {
  // Schema information.
  FFIColumnType col_types[64];
  char *col_names[128];
  // Counts.
  size_t num_rows;
  size_t num_cols;
  // The actual size of this is num_rows * num_cols.
  // The value of col j and row i corresponds to values[i * num_cols + j].
  FFIRecord records[];
} FFIResult;

typedef struct {
  size_t stmt_id;
  size_t arg_count;
  FFIColumnType args[];
} FFIPreparedStatement;

// The FFI API.

// Pass command line arguments to gflags
FFIArgs FFIGflags(int argc, char **argv, const char *usage);

// Initialize pelton_state in pelton.cc. Returns true if successful and false
// otherwise.
bool FFIInitialize(size_t workers, bool consistent);

// Open a connection for a single client. The returned struct has connected =
// true if successful. Otherwise connected = false.
FFIConnection FFIOpen(const char *db_name);

// Execute a DDL statement (e.g. CREATE TABLE, CREATE VIEW, CREATE INDEX).
bool FFIExecDDL(FFIConnection *c_conn, const char *query);

// Execute an update statement (e.g. INSERT, UPDATE, DELETE).
// Returns -1 if error, otherwise returns the number of affected rows.
int FFIExecUpdate(FFIConnection *c_conn, const char *query);

// Executes a query (SELECT).
// Returns nullptr (0) on error.
FFIResult *FFIExecSelect(FFIConnection *c_conn, const char *query);

// Clean up the memory allocated by an FFIResult.
void FFIDestroySelect(FFIResult *c_result);

// Delete pelton_state. Returns true if successful and false otherwise.
bool FFIShutdown();

// Shutdown planner.
void FFIPlannerShutdown();

// Close a client connection. Returns true if successful and false otherwise.
bool FFIClose(FFIConnection *c_conn);

// Create a prepared statement.
FFIPreparedStatement *FFIPrepare(FFIConnection *c_conn, const char *query);
void FFIDestroyPreparedStatement(FFIPreparedStatement *c_stmt);

// Execute a prepared statement.
FFIResult *FFIExecPrepare(FFIConnection *c_conn, size_t stmt_id,
                          size_t arg_count, const char **arg_values);

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // PELTON_PROXY_SRC_FFI_FFI_H_