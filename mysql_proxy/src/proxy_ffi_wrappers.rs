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
  pub db_name: String,
  pub db_username: String,
  pub db_password: String
}

// Custom destructor
impl Drop for FFIResult {
    fn drop(&mut self) {
        unsafe {FFIDestroySelect(self)};
    }
}

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
    let db_username = unsafe { CStr::from_ptr(flags.db_username) };
    let db_username = db_username.to_str().unwrap();
    let db_password = unsafe { CStr::from_ptr(flags.db_password) };
    let db_password = db_password.to_str().unwrap();
    return CommandLineArgs {
        workers: flags.workers,
        db_name: db_name.to_string(),
        db_username: db_username.to_string(),
        db_password: db_password.to_string()
    };
}

pub fn initialize_ffi(workers: usize) -> bool {
    return unsafe { FFIInitialize(workers) };
}

pub fn open_ffi(db: &str, user: &str, pass: &str) -> FFIConnection {
    let db = CString::new(db).unwrap();
    let db = db.as_ptr();
    let user = CString::new(user).unwrap();
    let user = user.as_ptr();
    let pass = CString::new(pass).unwrap();
    let pass = pass.as_ptr();
    return unsafe { FFIOpen(db, user, pass) };
}

pub fn shutdown_ffi() -> bool {
    return unsafe { FFIShutdown() };
}

pub fn close_ffi(rust_conn: *mut FFIConnection) -> bool {
    return unsafe { FFIClose(rust_conn) };
}

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
        let col_type_c = col_types[c];
        let col_type = match col_type_c {
            FFIColumnType_UINT => ColumnType::MYSQL_TYPE_LONGLONG,
            FFIColumnType_INT => ColumnType::MYSQL_TYPE_LONGLONG,
            FFIColumnType_TEXT => ColumnType::MYSQL_TYPE_VAR_STRING,
            FFIColumnType_DATETIME => ColumnType::MYSQL_TYPE_VAR_STRING,
            _ => ColumnType::MYSQL_TYPE_NULL,
        };
        cols.push(Column {
            table: "".to_string(),
            column: col_name_string,
            coltype: col_type,
            colflags: ColumnFlags::empty(),
        });
    }
    return cols;
}
