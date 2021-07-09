/* automatically generated by rust-bindgen 0.58.1 */

#[repr(C)]
#[derive(Default)]
pub struct __IncompleteArrayField<T>(::std::marker::PhantomData<T>, [T; 0]);
impl<T> __IncompleteArrayField<T> {
    #[inline]
    pub const fn new() -> Self {
        __IncompleteArrayField(::std::marker::PhantomData, [])
    }
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self as *const _ as *const T
    }
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self as *mut _ as *mut T
    }
    #[inline]
    pub unsafe fn as_slice(&self, len: usize) -> &[T] {
        ::std::slice::from_raw_parts(self.as_ptr(), len)
    }
    #[inline]
    pub unsafe fn as_mut_slice(&mut self, len: usize) -> &mut [T] {
        ::std::slice::from_raw_parts_mut(self.as_mut_ptr(), len)
    }
}
impl<T> ::std::fmt::Debug for __IncompleteArrayField<T> {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        fmt.write_str("__IncompleteArrayField")
    }
}
pub const true_: u32 = 1;
pub const false_: u32 = 0;
pub const __bool_true_false_are_defined: u32 = 1;
#[repr(C)]
pub struct CResult {
    pub col_names: [[::std::os::raw::c_char; 64usize]; 64usize],
    pub col_types: [[::std::os::raw::c_char; 64usize]; 64usize],
    pub num_rows: ::std::os::raw::c_ulong,
    pub num_cols: ::std::os::raw::c_ulong,
    pub records: __IncompleteArrayField<*mut CResult_RecordData>,
}
#[repr(C)]
#[derive(Copy, Clone)]
pub union CResult_RecordData {
    pub UINT: ::std::os::raw::c_ulong,
    pub INT: ::std::os::raw::c_int,
}
#[test]
fn bindgen_test_layout_CResult_RecordData() {
    assert_eq!(
        ::std::mem::size_of::<CResult_RecordData>(),
        8usize,
        concat!("Size of: ", stringify!(CResult_RecordData))
    );
    assert_eq!(
        ::std::mem::align_of::<CResult_RecordData>(),
        8usize,
        concat!("Alignment of ", stringify!(CResult_RecordData))
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<CResult_RecordData>())).UINT as *const _ as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(CResult_RecordData),
            "::",
            stringify!(UINT)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<CResult_RecordData>())).INT as *const _ as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(CResult_RecordData),
            "::",
            stringify!(INT)
        )
    );
}
#[test]
fn bindgen_test_layout_CResult() {
    assert_eq!(
        ::std::mem::size_of::<CResult>(),
        8208usize,
        concat!("Size of: ", stringify!(CResult))
    );
    assert_eq!(
        ::std::mem::align_of::<CResult>(),
        8usize,
        concat!("Alignment of ", stringify!(CResult))
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<CResult>())).col_names as *const _ as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(CResult),
            "::",
            stringify!(col_names)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<CResult>())).col_types as *const _ as usize },
        4096usize,
        concat!(
            "Offset of field: ",
            stringify!(CResult),
            "::",
            stringify!(col_types)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<CResult>())).num_rows as *const _ as usize },
        8192usize,
        concat!(
            "Offset of field: ",
            stringify!(CResult),
            "::",
            stringify!(num_rows)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<CResult>())).num_cols as *const _ as usize },
        8200usize,
        concat!(
            "Offset of field: ",
            stringify!(CResult),
            "::",
            stringify!(num_cols)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<CResult>())).records as *const _ as usize },
        8208usize,
        concat!(
            "Offset of field: ",
            stringify!(CResult),
            "::",
            stringify!(records)
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


// custom destructors
impl Drop for CResult {
    fn drop(&mut self) {
        println!("Rust FFI: Calling destructor for CResult");
        // need to destruct entire struct since malloced it in C-Wrapper
        unsafe {destroy_select(self)};
        println!("Rust FFI: CResult destroyed");
    }
}

impl Drop for ConnectionC {
    fn drop (&mut self) {
        println!("Rust FFI: Calling destructor for ConnectionC");
        // the struct itself is destructed by rust automatically. Only
        // the C++ struct in the field of the rust struct is not and requires
        // manually deallocation
        unsafe {destroy_conn(self.cpp_conn)};
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
    pub fn exec_ddl(c_conn: *mut ConnectionC, query: *const ::std::os::raw::c_char) -> bool;
    pub fn exec_update(
        c_conn: *mut ConnectionC,
        query: *const ::std::os::raw::c_char,
    ) -> ::std::os::raw::c_int;
    pub fn exec_select(
        c_conn: *mut ConnectionC,
        query: *const ::std::os::raw::c_char,
    ) -> *mut CResult;
    pub fn close_conn(c_conn: *mut ConnectionC) -> bool;
    pub fn destroy_select(c_result: *mut CResult);
    pub fn destroy_conn(c_conn: *mut ::std::os::raw::c_void);
}

