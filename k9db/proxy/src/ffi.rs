pub mod k9db {
  // bindgen API is internal.
  #[allow(dead_code)]
  #[allow(non_upper_case_globals)]
  #[allow(non_camel_case_types)]
  #[allow(non_snake_case)]
  mod ffi {
    include!("ffi_bindgen.rs");
  }

  // Except for some structs that we want to expose publicly.
  pub use ffi::FFIColumnType;
  pub use ffi::FFIConnection;
  pub use ffi::FFIPreparedResult;
  pub use ffi::FFIPreparedStatement;
  pub use ffi::{FFIColumnType_DATETIME, FFIColumnType_INT, FFIColumnType_TEXT,
                FFIColumnType_UINT};
  pub use ffi::{FFIQueryResult, FFIUpdateResult};

  // Dependencies from ffi crate.
  use chrono::naive::NaiveDateTime;
  use std::ffi::{CStr, CString};
  use std::os::raw::{c_char, c_int};

  // Error type.
  pub struct K9dbError(pub u16, pub String);
  impl std::fmt::Display for K9dbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      write!(f,
             "Error code: {:?}, Error message: {}",
             msql_srv::ErrorKind::from(self.0),
             self.1)
    }
  }

  // Result type.
  pub type K9dbResult<T> = Result<T, K9dbError>;

  // Turn error into a rust string and free it, assumes buf is not null.
  fn read_and_free_error(error: ffi::FFIError) -> K9dbError {
    let code = error.code;
    let cstr = unsafe { CStr::from_ptr(error.message) };
    let error_string = cstr.to_str()
                           .map(|s| String::from(s))
                           .unwrap_or_else(|e| format!("{}", e));

    // Free error after copying content.
    unsafe { ffi::FFIFreeError(error) };

    K9dbError(code, error_string)
  }

  // Transforms FFI result on the form struct { T result, char *error }
  // to K9dbResult<T>.
  macro_rules! ToResult {
    ($x:ident) => {
      if $x.error.code == 0 {
        Ok($x.result)
      } else {
        Err(read_and_free_error($x.error))
      }
    };
  }

  // Command line flags.
  pub struct CommandLineArgs {
    pub workers: usize,
    pub consistent: bool,
    pub db_name: String,
    pub hostname: String,
    pub db_path: String,
  }

  // Parse command line flags using gflags in native FFI.
  pub fn gflags(args: std::env::Args,
                usage: &str)
                -> K9dbResult<CommandLineArgs> {
    let usage = CString::new(usage).unwrap();
    // Encode args as char**.
    let args = args.map(|arg| CString::new(arg).unwrap())
                   .collect::<Vec<CString>>();
    let mut c_argv = args.iter()
                         .map(|arg| arg.as_ptr() as *mut c_char)
                         .collect::<Vec<*mut c_char>>();
    // Pass to FFI.
    let c_len = args.len() as c_int;
    let c_argv = c_argv.as_mut_ptr();
    let c_usage = usage.as_ptr();
    let flags = unsafe { ffi::FFIGflags(c_len, c_argv, c_usage) };

    let flags = ToResult!(flags);
    // Transform FFIArgs to CommandLineArgs.
    flags.map(|flags| {
           let db_name = unsafe { CStr::from_ptr(flags.db_name) };
           let db_name = db_name.to_str().unwrap();
           let hostname = unsafe { CStr::from_ptr(flags.hostname) };
           let hostname = hostname.to_str().unwrap();
           let db_path = unsafe { CStr::from_ptr(flags.db_path) };
           let db_path = db_path.to_str().unwrap();
           CommandLineArgs { workers: flags.workers,
                             consistent: flags.consistent,
                             db_name: db_name.to_string(),
                             hostname: hostname.to_string(),
                             db_path: db_path.to_string() }
         })
  }

  // Starting and stopping the proxy.
  pub fn initialize(workers: usize,
                    consistent: bool,
                    db: &str,
                    path: &str)
                    -> K9dbResult<bool> {
    let db = CString::new(db).unwrap();
    let db = db.as_ptr();
    let path = CString::new(path).unwrap();
    let path = path.as_ptr();
    let result = unsafe { ffi::FFIInitialize(workers, consistent, db, path) };
    ToResult!(result)
  }
  pub fn shutdown() -> K9dbResult<bool> {
    let result = unsafe { ffi::FFIShutdown() };
    ToResult!(result)
  }
  pub fn shutdown_planner() {
    unsafe { ffi::FFIPlannerShutdown() }
  }

  // Open and close a new connection.
  pub fn open() -> K9dbResult<FFIConnection> {
    let result = unsafe { ffi::FFIOpen() };
    ToResult!(result)
  }
  pub fn close(rust_conn: FFIConnection) -> K9dbResult<bool> {
    let result = unsafe { ffi::FFIClose(rust_conn) };
    ToResult!(result)
  }

  // Handling different types of SQL statements.
  pub fn exec_ddl(rust_conn: FFIConnection, query: &str) -> K9dbResult<bool> {
    let c_query = CString::new(query).unwrap();
    let char_query: *const c_char = c_query.as_ptr();
    let result = unsafe { ffi::FFIExecDDL(rust_conn, char_query) };
    ToResult!(result)
  }

  pub fn exec_update(rust_conn: FFIConnection,
                     query: &str)
                     -> K9dbResult<FFIUpdateResult> {
    let c_query = CString::new(query).unwrap();
    let char_query: *const c_char = c_query.as_ptr();
    let result = unsafe { ffi::FFIExecUpdate(rust_conn, char_query) };
    ToResult!(result)
  }

  pub fn exec_select(rust_conn: FFIConnection,
                     query: &str)
                     -> K9dbResult<FFIQueryResult> {
    let c_query = CString::new(query).unwrap();
    let char_query: *const c_char = c_query.as_ptr();
    let result = unsafe { ffi::FFIExecSelect(rust_conn, char_query) };
    ToResult!(result)
  }

  // Prepared statements.
  pub fn prepare(rust_conn: FFIConnection,
                 query: &str)
                 -> K9dbResult<FFIPreparedStatement> {
    let c_query = CString::new(query).unwrap();
    let char_query: *const c_char = c_query.as_ptr();
    let result = unsafe { ffi::FFIPrepare(rust_conn, char_query) };
    ToResult!(result)
  }

  pub fn exec_prepare(rust_conn: FFIConnection,
                      stmt_id: u32,
                      args: Vec<String>)
                      -> K9dbResult<FFIPreparedResult> {
    let args_len = args.len();
    let mut cstr_args: Vec<CString> = Vec::with_capacity(args_len);
    let mut c_args: Vec<*const c_char> = Vec::with_capacity(args_len);
    let mut i = 0;
    for arg in args {
      cstr_args.push(CString::new(arg).unwrap());
      c_args.push(cstr_args[i].as_ptr());
      i += 1;
    }
    let result = unsafe {
      ffi::FFIExecPrepare(rust_conn,
                          stmt_id as usize,
                          args_len,
                          c_args.as_mut_ptr())
    };
    ToResult!(result)
  }

  // Functions to interact with FFIResult.
  pub mod result {
    use super::*;

    // Resultset.
    pub fn next_resultset(c_result: FFIQueryResult) -> bool {
      unsafe { ffi::FFIResultNextSet(c_result) }
    }

    // Schema.
    pub fn column_count(c_result: FFIQueryResult) -> usize {
      unsafe { ffi::FFIResultColumnCount(c_result) }
    }
    pub fn column_name<'a>(c_result: FFIQueryResult, col: usize) -> &'a str {
      let cstr = unsafe {
        let ptr = ffi::FFIResultColumnName(c_result, col);
        CStr::from_ptr(ptr)
      };
      cstr.to_str().unwrap()
    }
    pub fn column_type(c_result: FFIQueryResult, col: usize) -> FFIColumnType {
      unsafe { ffi::FFIResultColumnType(c_result, col) }
    }

    // Data.
    pub fn row_count(c_result: FFIQueryResult) -> usize {
      unsafe { ffi::FFIResultRowCount(c_result) }
    }
    pub fn null(c_result: FFIQueryResult, row: usize, col: usize) -> bool {
      unsafe { ffi::FFIResultIsNull(c_result, row, col) }
    }
    pub fn uint(c_result: FFIQueryResult, row: usize, col: usize) -> u64 {
      unsafe { ffi::FFIResultGetUInt(c_result, row, col) }
    }
    pub fn int(c_result: FFIQueryResult, row: usize, col: usize) -> i64 {
      unsafe { ffi::FFIResultGetInt(c_result, row, col) }
    }
    pub fn text<'a>(c_result: FFIQueryResult,
                    row: usize,
                    col: usize)
                    -> &'a str {
      let cstr = unsafe {
        let ptr = ffi::FFIResultGetString(c_result, row, col);
        CStr::from_ptr(ptr)
      };
      cstr.to_str().unwrap()
    }
    pub fn datetime<'a>(c_result: FFIQueryResult,
                        row: usize,
                        col: usize)
                        -> NaiveDateTime {
      let cstr = unsafe {
        let ptr = ffi::FFIResultGetDatetime(c_result, row, col);
        CStr::from_ptr(ptr)
      };
      let rstr = cstr.to_str().unwrap();
      NaiveDateTime::parse_from_str(rstr, "%Y-%m-%d %H:%M:%S%.f").unwrap()
    }

    // Destory.
    pub fn destroy(c_result: FFIQueryResult) {
      unsafe { ffi::FFIResultDestroy(c_result) };
    }
  }

  pub mod statement {
    use super::*;

    pub fn id(c_stmt: FFIPreparedStatement) -> u32 {
      unsafe { ffi::FFIPreparedStatementID(c_stmt) as u32 }
    }
    pub fn arg_count(c_stmt: FFIPreparedStatement) -> usize {
      unsafe { ffi::FFIPreparedStatementArgCount(c_stmt) }
    }
    pub fn arg_type(c_stmt: FFIPreparedStatement,
                    arg: usize)
                    -> K9dbResult<FFIColumnType> {
      let result = unsafe { ffi::FFIPreparedStatementArgType(c_stmt, arg) };
      ToResult!(result)
    }
  }
}
