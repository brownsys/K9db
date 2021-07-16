use std::ffi::CStr;
use std::ffi::CString;
use std::os::raw::c_char;
include!("proxy_ffi.rs");

fn open(dir: &str, user: &str, pass: &str) -> ConnectionC {
    // convert &str to char* to pass query to C-wrapper
    let dir = CString::new(dir).unwrap();
    let dir: *mut c_char = dir.as_ptr() as *mut i8;

    let user = CString::new(user).unwrap();
    let user: *mut c_char = user.as_ptr() as *mut i8;

    let pass = CString::new(pass).unwrap();
    let pass: *mut c_char = pass.as_ptr() as *mut i8;

    let conn = unsafe { open_c(dir, user, pass) };
    return conn;
}

fn close(rust_conn: *mut ConnectionC) -> bool {
    let response = unsafe { close_conn(rust_conn) };
    return response;
}

fn exec_ddl_rust(rust_conn: *mut ConnectionC, query: &str) -> bool {
    println!("Rust Proxy: ddl query received is: {:?}", query);
    let s = CString::new(query).unwrap();
    let c_query = s.as_ptr() as *mut i8;
    return unsafe { exec_ddl(rust_conn, c_query) };
}

fn exec_update_rust(rust_conn: *mut ConnectionC, query: &str) -> i32 {
    println!("Rust Proxy: update query received is: {:?}", query);
    let s = CString::new(query).unwrap();
    let c_query = s.as_ptr() as *mut i8;
    return unsafe { exec_update(rust_conn, c_query) };
}

fn exec_select_rust(rust_conn: *mut ConnectionC, query: &str) -> *mut CResult {
    println!("Rust Proxy: select query received is: {:?}", query);
    let s = CString::new(query).unwrap();
    let c_query = s.as_ptr() as *mut i8;
    return unsafe { exec_select(rust_conn, c_query) };
}
