#ifndef PELTON_PROXY_SRC_FFI_FFI_H_
#define PELTON_PROXY_SRC_FFI_FFI_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

// Command line arguments read via gflags.
typedef struct {
  size_t workers;
  bool consistent;
  const char *db_name;
  const char *hostname;
  const char *db_path;
} FFIArgs;

// In reality, this is of type pelton::ConnectionLocal.
typedef void *FFIConnection;

// Returned result for an Update.
typedef struct {
  int row_count;
  uint64_t last_insert_id;
} FFIUpdateResult;

// Our version of pelton/sqlast/ast_schema_enums.h#ColumnDefinitionTypeEnum.
typedef enum { UINT = 0, INT = 1, TEXT = 2, DATETIME = 3 } FFIColumnType;

// A pointer to a SqlResult but we use void * to simplify rust handling.
typedef struct {
  void *result;
  int *index;
} FFIResult;

// A pointer to PreparedStatementDescriptor but we use void * for rust.
typedef const void *FFIPreparedStatement;

// The result of executing a prepared statement.
typedef struct {
  bool query;  // if true, query_result is set.
  FFIResult query_result;
  int update_result;
  uint64_t last_insert_id;
} FFIPreparedResult;

// The FFI API.

// Pass command line arguments to gflags
FFIArgs FFIGflags(int argc, char **argv, const char *usage);

// Initialize pelton_state in pelton.cc. Returns true if successful and false
// otherwise.
bool FFIInitialize(size_t workers, bool consistent, const char *db_name,
                   const char *db_path);

// Open a connection for a single client. The returned struct has connected =
// true if successful. Otherwise connected = false.
FFIConnection FFIOpen();

// Execute a DDL statement (e.g. CREATE TABLE, CREATE VIEW, CREATE INDEX).
bool FFIExecDDL(FFIConnection c_conn, const char *query);

// Execute an update statement (e.g. INSERT, UPDATE, DELETE).
// Returns -1 if error, otherwise returns the number of affected rows.
FFIUpdateResult FFIExecUpdate(FFIConnection c_conn, const char *query);

// Executes a query (SELECT).
// Returns nullptr (0) on error.
FFIResult FFIExecSelect(FFIConnection c_conn, const char *query);

// Create a prepared statement.
FFIPreparedStatement FFIPrepare(FFIConnection c_conn, const char *query);

// Execute a prepared statement.
FFIPreparedResult FFIExecPrepare(FFIConnection c_conn, size_t stmt_id,
                                 size_t arg_count, const char **arg_values);

// Close a client connection. Returns true if successful and false otherwise.
bool FFIClose(FFIConnection c_conn);

// Shutdown planner.
void FFIPlannerShutdown();

// Delete pelton_state. Returns true if successful and false otherwise.
bool FFIShutdown();

// FFIResult schema handling.
bool FFIResultNextSet(FFIResult c_result);
size_t FFIResultColumnCount(FFIResult c_result);
const char *FFIResultColumnName(FFIResult c_result, size_t col);
FFIColumnType FFIResultColumnType(FFIResult c_result, size_t col);

// FFIResult data handling.
size_t FFIResultRowCount(FFIResult c_result);
bool FFIResultIsNull(FFIResult c_result, size_t row, size_t col);
uint64_t FFIResultGetUInt(FFIResult c_result, size_t row, size_t col);
int64_t FFIResultGetInt(FFIResult c_result, size_t row, size_t col);
const char *FFIResultGetString(FFIResult c_result, size_t row, size_t col);
const char *FFIResultGetDatetime(FFIResult c_result, size_t row, size_t col);

// Clean up the memory allocated by an FFIResult.
void FFIResultDestroy(FFIResult c_result);

// FFIPreparedStatement handling.
size_t FFIPreparedStatementID(FFIPreparedStatement c_stmt);
size_t FFIPreparedStatementArgCount(FFIPreparedStatement c_stmt);
FFIColumnType FFIPreparedStatementArgType(FFIPreparedStatement c_stmt,
                                          size_t arg);

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // PELTON_PROXY_SRC_FFI_FFI_H_
