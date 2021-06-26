/* automatically generated by rust-bindgen 0.58.1 */

pub const true_: u32 = 1;
pub const false_: u32 = 0;
pub const __bool_true_false_are_defined: u32 = 1;
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ConnectionC {
    pub cpp_conn: *mut ::std::os::raw::c_void,
    pub connected: bool,
}
#[test]
fn bindgen_test_layout_ConnectionC() {
    assert_eq!(
        ::std::mem::size_of::<ConnectionC>(),
        16usize,
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
#[link(name = "open")]
extern "C" {
    pub fn open_c(
        query: *mut ::std::os::raw::c_char,
        db_username: *mut ::std::os::raw::c_char,
        db_password: *mut ::std::os::raw::c_char,
    ) -> ConnectionC;
}
#[link(name = "open")]
extern "C" {
    pub fn destroy(c_conn: ConnectionC);
}
