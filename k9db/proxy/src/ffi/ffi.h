#ifndef K9DB_PROXY_SRC_FFI_FFI_H_
#define K9DB_PROXY_SRC_FFI_FFI_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

// An error is a string, nullptr means no error.
typedef struct {
  uint16_t code;  // 0 means no error.
  char *message;
} FFIError;

// Free error after use in rust.
void FFIFreeError(FFIError error);

// Command line arguments read via gflags.
typedef struct {
  size_t workers;
  bool consistent;
  const char *db_name;
  const char *hostname;
  const char *db_path;
} FFIArgs;

typedef struct {
  FFIArgs result;
  FFIError error;
} FFIArgsOrError;

// In reality, this is of type k9db::Connection.
typedef void *FFIConnection;

typedef struct {
  FFIConnection result;
  FFIError error;
} FFIConnectionOrError;

// Returned result for an Update.
typedef struct {
  int row_count;
  uint64_t last_insert_id;
} FFIUpdateResult;

typedef struct {
  FFIUpdateResult result;
  FFIError error;
} FFIUpdateResultOrError;

// Our version of k9db/sqlast/ast_schema_enums.h#ColumnDefinitionTypeEnum.
typedef enum { UINT = 0, INT = 1, TEXT = 2, DATETIME = 3 } FFIColumnType;

typedef struct {
  FFIColumnType result;
  FFIError error;
} FFIColumnTypeOrError;

// A pointer to a SqlResult but we use void * to simplify rust handling.
typedef struct {
  void *result;
  int *index;
} FFIQueryResult;

typedef struct {
  FFIQueryResult result;
  FFIError error;
} FFIQueryResultOrError;

// A pointer to PreparedStatementDescriptor but we use void * for rust.
typedef const void *FFIPreparedStatement;

typedef struct {
  FFIPreparedStatement result;
  FFIError error;
} FFIPreparedStatementOrError;

// The result of executing a prepared statement.
typedef struct {
  bool query;  // if true, query_result is set.
  FFIQueryResult query_result;
  FFIUpdateResult update_result;
} FFIPreparedResult;

typedef struct {
  FFIPreparedResult result;
  FFIError error;
} FFIPreparedResultOrError;

// The FFI API.

// Return types can also contain errors.
typedef struct {
  bool result;
  FFIError error;
} FFISuccessOrError;

// Pass command line arguments to gflags
FFIArgsOrError FFIGflags(int argc, char **argv, const char *usage);

// Initialize k9db_state in k9db.cc. Returns true if successful and false
// otherwise.
FFISuccessOrError FFIInitialize(size_t workers, bool consistent,
                                const char *db_name, const char *db_path);

// Open a connection for a single client. The returned struct has connected =
// true if successful. Otherwise connected = false.
FFIConnectionOrError FFIOpen();

// Execute a DDL statement (e.g. CREATE TABLE, CREATE VIEW, CREATE INDEX).
FFISuccessOrError FFIExecDDL(FFIConnection c_conn, const char *query);

// Execute an update statement (e.g. INSERT, UPDATE, DELETE).
// Returns -1 if error, otherwise returns the number of affected rows.
FFIUpdateResultOrError FFIExecUpdate(FFIConnection c_conn, const char *query);

// Executes a query (SELECT).
// Returns nullptr (0) on error.
FFIQueryResultOrError FFIExecSelect(FFIConnection c_conn, const char *query);

// Create a prepared statement.
FFIPreparedStatementOrError FFIPrepare(FFIConnection c_conn, const char *query);

// Execute a prepared statement.
FFIPreparedResultOrError FFIExecPrepare(FFIConnection c_conn, size_t stmt_id,
                                        size_t arg_count,
                                        const char **arg_values);

// Close a client connection. Returns true if successful and false otherwise.
FFISuccessOrError FFIClose(FFIConnection c_conn);

// Shutdown planner.
void FFIPlannerShutdown();

// Delete k9db_state. Returns true if successful and false otherwise.
FFISuccessOrError FFIShutdown();

// FFIResult schema handling.
bool FFIResultNextSet(FFIQueryResult c_result);
const char *FFIResultTableName(FFIQueryResult c_result);
size_t FFIResultColumnCount(FFIQueryResult c_result);
const char *FFIResultColumnName(FFIQueryResult c_result, size_t col);
FFIColumnType FFIResultColumnType(FFIQueryResult c_result, size_t col);

// FFIResult data handling.
size_t FFIResultRowCount(FFIQueryResult c_result);
bool FFIResultIsNull(FFIQueryResult c_result, size_t row, size_t col);
uint64_t FFIResultGetUInt(FFIQueryResult c_result, size_t row, size_t col);
int64_t FFIResultGetInt(FFIQueryResult c_result, size_t row, size_t col);
const char *FFIResultGetString(FFIQueryResult c_result, size_t row, size_t col);
const char *FFIResultGetDatetime(FFIQueryResult c_result, size_t row,
                                 size_t col);

// Clean up the memory allocated by an FFIResult.
void FFIResultDestroy(FFIQueryResult c_result);

// FFIPreparedStatement handling.
size_t FFIPreparedStatementID(FFIPreparedStatement c_stmt);
size_t FFIPreparedStatementArgCount(FFIPreparedStatement c_stmt);
FFIColumnTypeOrError FFIPreparedStatementArgType(FFIPreparedStatement c_stmt,
                                                 size_t arg);

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // K9DB_PROXY_SRC_FFI_FFI_H_
