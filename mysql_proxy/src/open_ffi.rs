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
pub type size_t = ::std::os::raw::c_ulong;
pub type wchar_t = ::std::os::raw::c_int;
#[repr(C)]
#[repr(align(16))]
#[derive(Debug, Copy, Clone)]
pub struct max_align_t {
    pub __clang_max_align_nonce1: ::std::os::raw::c_longlong,
    pub __bindgen_padding_0: u64,
    pub __clang_max_align_nonce2: u128,
}
#[test]
fn bindgen_test_layout_max_align_t() {
    assert_eq!(
        ::std::mem::size_of::<max_align_t>(),
        32usize,
        concat!("Size of: ", stringify!(max_align_t))
    );
    assert_eq!(
        ::std::mem::align_of::<max_align_t>(),
        16usize,
        concat!("Alignment of ", stringify!(max_align_t))
    );
    assert_eq!(
        unsafe {
            &(*(::std::ptr::null::<max_align_t>())).__clang_max_align_nonce1 as *const _ as usize
        },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(max_align_t),
            "::",
            stringify!(__clang_max_align_nonce1)
        )
    );
    assert_eq!(
        unsafe {
            &(*(::std::ptr::null::<max_align_t>())).__clang_max_align_nonce2 as *const _ as usize
        },
        16usize,
        concat!(
            "Offset of field: ",
            stringify!(max_align_t),
            "::",
            stringify!(__clang_max_align_nonce2)
        )
    );
}
pub const ColumnDefinitionTypeEnum_UINT: ColumnDefinitionTypeEnum = 0;
pub const ColumnDefinitionTypeEnum_INT: ColumnDefinitionTypeEnum = 1;
pub const ColumnDefinitionTypeEnum_TEXT: ColumnDefinitionTypeEnum = 2;
pub const ColumnDefinitionTypeEnum_DATETIME: ColumnDefinitionTypeEnum = 3;
pub type ColumnDefinitionTypeEnum = ::std::os::raw::c_uint;
#[repr(C)]
pub struct CResult {
    pub col_names: [*mut ::std::os::raw::c_char; 64usize],
    pub col_types: [ColumnDefinitionTypeEnum; 64usize],
    pub num_rows: size_t,
    pub num_cols: size_t,
    pub records: __IncompleteArrayField<CResult_RecordData>,
}
#[repr(C)]
#[derive(Copy, Clone)]
pub union CResult_RecordData {
    pub UINT: ::std::os::raw::c_ulong,
    pub INT: ::std::os::raw::c_int,
    pub TEXT: *mut ::std::os::raw::c_char,
    pub DATETIME: *mut ::std::os::raw::c_char,
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
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<CResult_RecordData>())).TEXT as *const _ as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(CResult_RecordData),
            "::",
            stringify!(TEXT)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<CResult_RecordData>())).DATETIME as *const _ as usize },
        0usize,
        concat!(
            "Offset of field: ",
            stringify!(CResult_RecordData),
            "::",
            stringify!(DATETIME)
        )
    );
}
#[test]
fn bindgen_test_layout_CResult() {
    assert_eq!(
        ::std::mem::size_of::<CResult>(),
        784usize,
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
        512usize,
        concat!(
            "Offset of field: ",
            stringify!(CResult),
            "::",
            stringify!(col_types)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<CResult>())).num_rows as *const _ as usize },
        768usize,
        concat!(
            "Offset of field: ",
            stringify!(CResult),
            "::",
            stringify!(num_rows)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<CResult>())).num_cols as *const _ as usize },
        776usize,
        concat!(
            "Offset of field: ",
            stringify!(CResult),
            "::",
            stringify!(num_cols)
        )
    );
    assert_eq!(
        unsafe { &(*(::std::ptr::null::<CResult>())).records as *const _ as usize },
        784usize,
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

