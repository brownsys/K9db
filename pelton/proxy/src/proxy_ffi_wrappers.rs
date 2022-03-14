#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

extern crate msql_srv;

use msql_srv::{ColumnType, Column, ColumnFlags};
use std::ffi::{CString, CStr};
use std::os::raw::{c_char, c_int};

include!("proxy_ffi_bindgen.rs");

pub struct CommandLineArgs {
  pub workers: usize,
  pub consistent: bool,
  pub db_name: String,
  pub hostname: String,
}

// Custom destructor
impl Drop for FFIResult {
    fn drop(&mut self) {
        unsafe {FFIDestroySelect(self)};
    }
}
impl Drop for FFIPreparedStatement {
    fn drop(&mut self) {
        unsafe {FFIDestroyPreparedStatement(self)};
    }
}

// Command line arguments.
pub fn gflags_ffi(args: std::env::Args, usage: &str) -> CommandLineArgs {
    let usage = CString::new(usage).unwrap();
    // Encode args as char**.
    let args = args.map(|arg| CString::new(arg).unwrap()).collect::<Vec<CString>>();
    let mut c_argv = args
        .iter()
        .map(|arg| arg.as_ptr() as *mut c_char)
        .collect::<Vec<*mut c_char>>();
    // Pass to FFI.
    let flags = unsafe { FFIGflags(args.len() as c_int, c_argv.as_mut_ptr(), usage.as_ptr()) };
    // Transform FFIArgs to CommandLineArgs.
    let db_name = unsafe { CStr::from_ptr(flags.db_name) };
    let db_name = db_name.to_str().unwrap();
    let hostname = unsafe { CStr::from_ptr(flags.hostname) };
    let hostname = hostname.to_str().unwrap();
    return CommandLineArgs {
        workers: flags.workers,
        consistent: flags.consistent,
        db_name: db_name.to_string(),
        hostname: hostname.to_string(),
    };
}

// Starting and stopping the proxy.
pub fn initialize_ffi(workers: usize, consistent: bool) -> bool {
    return unsafe { FFIInitialize(workers, consistent) };
}
pub fn shutdown_ffi() -> bool {
    return unsafe { FFIShutdown() };
}
pub fn shutdown_planner_ffi() {
    unsafe { FFIPlannerShutdown() }
}

// Open and close a new connection.
pub fn open_ffi(db: &str) -> FFIConnection {
    let db = CString::new(db).unwrap();
    let db = db.as_ptr();
    return unsafe { FFIOpen(db) };
}
pub fn close_ffi(rust_conn: *mut FFIConnection) -> bool {
    return unsafe { FFIClose(rust_conn) };
}

// Handling different types of SQL statements.
pub fn exec_ddl_ffi(rust_conn: *mut FFIConnection, query: &str) -> bool {
    let c_query = CString::new(query).unwrap();
    let char_query: *const c_char = c_query.as_ptr();
    return unsafe { FFIExecDDL(rust_conn, char_query) };
}

pub fn exec_update_ffi(rust_conn: *mut FFIConnection, query: &str) -> i32 {
    let c_query = CString::new(query).unwrap();
    let char_query: *const c_char = c_query.as_ptr();
    return unsafe { FFIExecUpdate(rust_conn, char_query) };
}

pub fn exec_select_ffi(rust_conn: *mut FFIConnection, query: &str) -> *mut FFIResult {
    let c_query = CString::new(query).unwrap();
    let char_query: *const c_char = c_query.as_ptr();
    return unsafe { FFIExecSelect(rust_conn, char_query) };
}

pub fn convert_column_type(coltype: FFIColumnType) -> ColumnType {
    match coltype {
        FFIColumnType_UINT => ColumnType::MYSQL_TYPE_LONGLONG,
        FFIColumnType_INT => ColumnType::MYSQL_TYPE_LONGLONG,
        FFIColumnType_TEXT => ColumnType::MYSQL_TYPE_VAR_STRING,
        FFIColumnType_DATETIME => ColumnType::MYSQL_TYPE_VAR_STRING,
        _ => ColumnType::MYSQL_TYPE_NULL,
    }
}

pub fn convert_columns(
    num_cols: usize,
    col_types: [FFIColumnType; 64usize],
    col_names: [*mut ::std::os::raw::c_char; 128usize],
) -> Vec<Column> {
    let mut cols = Vec::with_capacity(num_cols);
    for c in 0..num_cols {
        // convert col_name at index c to rust string
        let col_name_string: String =
            unsafe { CString::from_raw(col_names[c]).into_string().unwrap() };

        // convert C column type enum to MYSQL type
        let col_type = convert_column_type(col_types[c]);
        cols.push(Column {
            table: "".to_string(),
            column: col_name_string,
            coltype: col_type,
            colflags: ColumnFlags::empty(),
        });
    }
    return cols;
}

// Prepared statements.
pub fn prepare_ffi(rust_conn: *mut FFIConnection, query: &str) -> *mut FFIPreparedStatement {
    let c_query = CString::new(query).unwrap();
    let char_query: *const c_char = c_query.as_ptr();
    return unsafe { FFIPrepare(rust_conn, char_query) };
}

pub fn exec_prepared_ffi(rust_conn: *mut FFIConnection, stmt_id: usize, args: Vec<String>) -> FFIPreparedResult {
    let args_len = args.len();
    let mut cstr_args: Vec<CString> = Vec::with_capacity(args_len);
    let mut c_args: Vec<* const c_char> = Vec::with_capacity(args_len);
    let mut i = 0;
    for arg in args {
      cstr_args.push(CString::new(arg).unwrap());
      c_args.push(cstr_args[i].as_ptr());
      i += 1;
    }
    return unsafe { FFIExecPrepare(rust_conn, stmt_id, args_len, c_args.as_mut_ptr()) };
}
