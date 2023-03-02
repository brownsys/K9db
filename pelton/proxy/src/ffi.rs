pub mod pelton {
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
  pub use ffi::{FFIResult, FFIUpdateResult};
  pub use ffi::{FFIColumnType_DATETIME, FFIColumnType_INT, FFIColumnType_TEXT,
                FFIColumnType_UINT};

  // Dependencies from ffi crate.
  use std::ffi::{CStr, CString};
  use std::os::raw::{c_char, c_int};

  // Command line flags.
  pub struct CommandLineArgs {
    pub workers: usize,
    pub consistent: bool,
    pub db_name: String,
    pub hostname: String,
    pub db_path: String,
  }

  // Parse command line flags using gflags in native FFI.
  pub fn gflags(args: std::env::Args, usage: &str) -> CommandLineArgs {
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
    // Transform FFIArgs to CommandLineArgs.
    let db_name = unsafe { CStr::from_ptr(flags.db_name) };
    let db_name = db_name.to_str().unwrap();
    let hostname = unsafe { CStr::from_ptr(flags.hostname) };
    let hostname = hostname.to_str().unwrap();
    let db_path = unsafe { CStr::from_ptr(flags.db_path) };
    let db_path = db_path.to_str().unwrap();
    return CommandLineArgs { workers: flags.workers,
                             consistent: flags.consistent,
                             db_name: db_name.to_string(),
                             hostname: hostname.to_string(),
                             db_path: db_path.to_string() };
  }

  // Starting and stopping the proxy.
  pub fn initialize(workers: usize, consistent: bool, db: &str, path: &str) -> bool {
    let db = CString::new(db).unwrap();
    let db = db.as_ptr();
    let path = CString::new(path).unwrap();
    let path = path.as_ptr();
    return unsafe { ffi::FFIInitialize(workers, consistent, db, path) };
  }
  pub fn shutdown() -> bool {
    return unsafe { ffi::FFIShutdown() };
  }
  pub fn shutdown_planner() {
    unsafe { ffi::FFIPlannerShutdown() }
  }

  // Open and close a new connection.
  pub fn open() -> FFIConnection {
    return unsafe { ffi::FFIOpen() };
  }
  pub fn close(rust_conn: FFIConnection) -> bool {
    return unsafe { ffi::FFIClose(rust_conn) };
  }

  // Handling different types of SQL statements.
  pub fn exec_ddl(rust_conn: FFIConnection, query: &str) -> bool {
    let c_query = CString::new(query).unwrap();
    let char_query: *const c_char = c_query.as_ptr();
    return unsafe { ffi::FFIExecDDL(rust_conn, char_query) };
  }

  pub fn exec_update(rust_conn: FFIConnection, query: &str) -> FFIUpdateResult {
    let c_query = CString::new(query).unwrap();
    let char_query: *const c_char = c_query.as_ptr();
    return unsafe { ffi::FFIExecUpdate(rust_conn, char_query) };
  }

  pub fn exec_select(rust_conn: FFIConnection, query: &str) -> FFIResult {
    let c_query = CString::new(query).unwrap();
    let char_query: *const c_char = c_query.as_ptr();
    return unsafe { ffi::FFIExecSelect(rust_conn, char_query) };
  }

  // Prepared statements.
  pub fn prepare(rust_conn: FFIConnection,
                 query: &str)
                 -> FFIPreparedStatement {
    let c_query = CString::new(query).unwrap();
    let char_query: *const c_char = c_query.as_ptr();
    return unsafe { ffi::FFIPrepare(rust_conn, char_query) };
  }

  pub fn exec_prepare(rust_conn: FFIConnection,
                      stmt_id: u32,
                      args: Vec<String>)
                      -> FFIPreparedResult {
    let args_len = args.len();
    let mut cstr_args: Vec<CString> = Vec::with_capacity(args_len);
    let mut c_args: Vec<*const c_char> = Vec::with_capacity(args_len);
    let mut i = 0;
    for arg in args {
      cstr_args.push(CString::new(arg).unwrap());
      c_args.push(cstr_args[i].as_ptr());
      i += 1;
    }
    return unsafe {
      ffi::FFIExecPrepare(rust_conn,
                          stmt_id as usize,
                          args_len,
                          c_args.as_mut_ptr())
    };
  }

  // Functions to interact with FFIResult.
  pub mod result {
    use super::*;

    // Resultset.
    pub fn next_resultset(c_result: FFIResult) -> bool {
      unsafe { ffi::FFIResultNextSet(c_result) }
    }

    // Schema.
    pub fn column_count(c_result: FFIResult) -> usize {
      unsafe { ffi::FFIResultColumnCount(c_result) }
    }
    pub fn column_name<'a>(c_result: FFIResult, col: usize) -> &'a str {
      let cstr = unsafe {
        let ptr = ffi::FFIResultColumnName(c_result, col);
        CStr::from_ptr(ptr)
      };
      cstr.to_str().unwrap()
    }
    pub fn column_type(c_result: FFIResult, col: usize) -> FFIColumnType {
      unsafe { ffi::FFIResultColumnType(c_result, col) }
    }

    // Data.
    pub fn row_count(c_result: FFIResult) -> usize {
      unsafe { ffi::FFIResultRowCount(c_result) }
    }
    pub fn null(c_result: FFIResult, row: usize, col: usize) -> bool {
      unsafe { ffi::FFIResultIsNull(c_result, row, col) }
    }
    pub fn uint(c_result: FFIResult, row: usize, col: usize) -> u64 {
      unsafe { ffi::FFIResultGetUInt(c_result, row, col) }
    }
    pub fn int(c_result: FFIResult, row: usize, col: usize) -> i64 {
      unsafe { ffi::FFIResultGetInt(c_result, row, col) }
    }
    pub fn text<'a>(c_result: FFIResult, row: usize, col: usize) -> &'a str {
      let cstr = unsafe {
        let ptr = ffi::FFIResultGetString(c_result, row, col);
        CStr::from_ptr(ptr)
      };
      cstr.to_str().unwrap()
    }
    pub fn datetime<'a>(c_result: FFIResult,
                        row: usize,
                        col: usize)
                        -> &'a str {
      let cstr = unsafe {
        let ptr = ffi::FFIResultGetDatetime(c_result, row, col);
        CStr::from_ptr(ptr)
      };
      cstr.to_str().unwrap()
    }

    // Destory.
    pub fn destroy(c_result: FFIResult) {
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
    pub fn arg_type(c_stmt: FFIPreparedStatement, arg: usize) -> FFIColumnType {
      unsafe { ffi::FFIPreparedStatementArgType(c_stmt, arg) }
    }
  }
}
