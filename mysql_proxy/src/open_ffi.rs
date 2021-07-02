/* automatically generated by rust-bindgen 0.58.1 */

pub const true_: u32 = 1;
pub const false_: u32 = 0;
pub const __bool_true_false_are_defined: u32 = 1;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct QueryResponse {
    pub response_type: *const ::std::os::raw::c_char,
    pub ddl: bool,
    pub update: ::std::os::raw::c_int,
}
#[test]
fn bindgen_test_layout_QueryResponse() {
    assert_eq!(
        ::std::mem::size_of::<QueryResponse>(),
        16usize,
        concat!("Size of: ", stringify!(QueryResponse))
    );
    assert_eq!(
        ::std::mem::align_of::<QueryResponse>(),
        8usize,
        concat!("Alignment of ", stringify!(QueryResponse))
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<QueryResponse>())).response_type as *const _ as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(QueryResponse),
            "::",
            stringify!(response_type)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<QueryResponse>())).ddl as *const _ as usize },
        8usize,
        concat!(
            "Offset of field: ",
            stringify!(QueryResponse),
            "::",
            stringify!(ddl)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<QueryResponse>())).update as *const _ as usize },
        12usize,
        concat!(
            "Offset of field: ",
            stringify!(QueryResponse),
            "::",
            stringify!(update)
        )
    );
}

#[repr(C)]
#[derive(Debug, Clone)]
pub struct ConnectionC {
    pub cpp_conn: *mut ::std::os::raw::c_void,
    pub connected: bool,
}
#[test]
fn bindgen_test_layout_ConnectionC() {
    assert_eq!(
        ::std::mem::size_of::<ConnectionC>(),
        32usize,
        concat!("Size of: ", stringify!(ConnectionC))
    );
    assert_eq!(
        ::std::mem::align_of::<ConnectionC>(),
        8usize,
        concat!("Alignment of ", stringify!(ConnectionC))
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<ConnectionC>())).cpp_conn as *const _ as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(ConnectionC),
            "::",
            stringify!(cpp_conn)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<ConnectionC>())).connected as *const _ as usize },
        8usize,
        concat!(
            "Offset of field: ",
            stringify!(ConnectionC),
            "::",
            stringify!(connected)
        )
    );
}

// custom destructor
impl Drop for ConnectionC {
    fn drop (&mut self) {
        println!("Rust FFI: Calling destructor for ConnectionC");
        // the struct itself is destructed by rust automatically. Only
        // the C++ struct in the field of the rust struct is not and requires
        // manually deallocation
        unsafe {destroy(self.cpp_conn)};
        println!("Rust FFI: ConnectionC destroyed");
    }
}

#[link(name = "open")]
extern "C" {
    pub fn open_c(
        query: *mut ::std::os::raw::c_char,
        db_username: *mut ::std::os::raw::c_char,
        db_password: *mut ::std::os::raw::c_char,
    ) -> ConnectionC;
    pub fn exec_c(c_conn: *mut ConnectionC, query: *const ::std::os::raw::c_char) -> QueryResponse;
    pub fn close_c(c_conn: *mut ConnectionC) -> bool;
    pub fn destroy(c_conn: *mut ::std::os::raw::c_void);
}

