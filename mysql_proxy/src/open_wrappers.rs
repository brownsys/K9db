use std::ffi::CStr;
use std::ffi::CString;
use std::os::raw::c_char;

include!("open_ffi.rs");

fn open(dir: &str, user: &str, pass: &str) -> ConnectionC {
    // Convert to *mut i8 to send to C-function
    let dir = CString::new(dir).unwrap();
    let dir: *mut c_char = dir.as_ptr() as *mut i8;
    // Convert to *mut i8 to send to C-function
    let user = CString::new(user).unwrap();
    let user: *mut c_char = user.as_ptr() as *mut i8;
    // Convert to *mut i8 to send to C-function
    let pass = CString::new(pass).unwrap();
    let pass: *mut c_char = pass.as_ptr() as *mut i8;

    let conn = unsafe { open_c(dir, user, pass) };
    return conn;
}

fn close(rust_conn: *mut ConnectionC) -> bool {
    let response = unsafe { close_conn(rust_conn) };
    return response;
}

union exec_type {
    ddl: bool,
    update: i32,
    select: *mut CResult,
}

fn exec(rust_conn: *mut ConnectionC, query: &str) -> exec_type {
    // convert &str to char* to pass query to C-wrapper
    let c_query = CString::new(query).unwrap();
    let c_query: *mut c_char = c_query.as_ptr() as *mut i8;

    // call appropriate C-wrapper for exec depending on type of query
    let response = if query.contains("CREATE") {
        println!("Rust Wrapper: query type is: ddl");
        unsafe {
            exec_type {
                ddl: exec_ddl(rust_conn, c_query),
            }
        }
    } else if query.contains("DELETE") || query.contains("INSERT") || query.contains("UPDATE") {
        println!("Rust Wrapper: query type is: update");
        unsafe {
            exec_type {
                update: exec_update(rust_conn, c_query),
            }
        }
    } else if query.contains("SELECT") {
        println!("Rust Wrapper: query type is: select");
        unsafe {
            exec_type {
                select: exec_select(rust_conn, c_query),
            }
        }
    } else {
        println!("Rust Wrapper: query type is: INVALID");
        exec_type { update: 1 }
    };
    println!("Rust Wrapper: about to return response to Rust Proxy main.rs");
    return response;
}
