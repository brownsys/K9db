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
    let c_query = CString::new(query).unwrap();
    let char_query = c_query.as_ptr() as *mut i8;
    return unsafe { exec_ddl(rust_conn, char_query) };
}

fn exec_update_rust(rust_conn: *mut ConnectionC, query: &str) -> i32 {
    println!("Rust Proxy: update query received is: {:?}", query);
    let c_query = CString::new(query).unwrap();
    let char_query = c_query.as_ptr() as *mut i8;
    return unsafe { exec_update(rust_conn, char_query) };
}

fn exec_select_rust(rust_conn: *mut ConnectionC, query: &str) -> *mut CResult {
    println!("Rust Proxy: select query received is: {:?}", query);
    let c_query = CString::new(query).unwrap();
    let char_query = c_query.as_ptr() as *mut i8;
    return unsafe { exec_select(rust_conn, char_query) };
}

fn convert_columns(
    num_cols: usize,
    col_types: [ColumnDefinitionTypeEnum; 64usize],
    col_names: [*mut ::std::os::raw::c_char; 128usize],
) -> Vec<Column> {
    let mut cols = Vec::with_capacity(num_cols);
    for c in 0..num_cols {
        // convert col_name at index c to rust string
        let col_name_string: String =
            unsafe { CStr::from_ptr(col_names[c]).to_str().unwrap().to_owned() };

        // convert C column type enum to MYSQL type
        let col_type_c = col_types[c];
        let col_type = match col_type_c {
            ColumnDefinitionTypeEnum_UINT => ColumnType::MYSQL_TYPE_LONGLONG,
            ColumnDefinitionTypeEnum_INT => ColumnType::MYSQL_TYPE_LONGLONG,
            ColumnDefinitionTypeEnum_TEXT => ColumnType::MYSQL_TYPE_VAR_STRING,
            ColumnDefinitionTypeEnum_DATETIME => ColumnType::MYSQL_TYPE_VAR_STRING,
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
