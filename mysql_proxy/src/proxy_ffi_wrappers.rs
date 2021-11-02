#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

extern crate msql_srv;

use msql_srv::{ColumnType, Column, ColumnFlags};
use std::ffi::CString;
use std::os::raw::c_char;

include!("proxy_ffi_bindgen.rs");

// Custom destructor
impl Drop for FFIResult {
    fn drop(&mut self) {
        unsafe {FFIDestroySelect(self)};
    }
}

pub fn initialize_ffi(dir: &str) -> bool {
    // convert &str to char* to pass query to C-wrapper
    let dir = CString::new(dir).unwrap();
    let dir: *mut c_char = dir.as_ptr() as *mut i8;

    let conn = unsafe { FFIInitialize(dir) };
    return conn;
}

pub fn open_ffi(db: &str, user: &str, pass: &str) -> FFIConnection {
    let db = CString::new(db).unwrap();
    let db: *mut c_char = db.as_ptr() as *mut i8;
    let user = CString::new(user).unwrap();
    let user: *mut c_char = user.as_ptr() as *mut i8;
    let pass = CString::new(pass).unwrap();
    let pass: *mut c_char = pass.as_ptr() as *mut i8;

    let conn = unsafe { FFIOpen(db, user, pass) };
    return conn;
}

pub fn shutdown_ffi() -> bool {
    let response = unsafe { FFIShutdown() };
    return response;
}

pub fn close_ffi(rust_conn: *mut FFIConnection) -> bool {
    let response = unsafe { FFIClose(rust_conn) };
    return response;
}

pub fn exec_ddl_ffi(rust_conn: *mut FFIConnection, query: &str) -> bool {
    let c_query = CString::new(query).unwrap();
    let char_query = c_query.as_ptr() as *mut i8;
    return unsafe { FFIExecDDL(rust_conn, char_query) };
}

pub fn exec_update_ffi(rust_conn: *mut FFIConnection, query: &str) -> i32 {
    let c_query = CString::new(query).unwrap();
    let char_query = c_query.as_ptr() as *mut i8;
    return unsafe { FFIExecUpdate(rust_conn, char_query) };
}

pub fn exec_select_ffi(rust_conn: *mut FFIConnection, query: &str) -> *mut FFIResult {
    let c_query = CString::new(query).unwrap();
    let char_query = c_query.as_ptr() as *mut i8;
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
