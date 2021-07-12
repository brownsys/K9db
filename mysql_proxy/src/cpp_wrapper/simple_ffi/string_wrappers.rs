use std::ffi::CStr;
use std::ffi::CString;
use std::os::raw::c_char;

include!("string_ffi.rs");

// convert rust string to C, call C-wrapper, convert C++ response to rust string
fn send_string(s: &str) -> &str {
    // Convert to *mut i8 to send to C-function
    let c_query = CString::new(s).unwrap();
    let c_query: *mut c_char = c_query.as_ptr() as *mut i8;

    // Receive *mut i8 back from C and convert to rust string ('move' so rust gets ownership of the heap allocated string)
    let query_response: *mut c_char = unsafe { query_pelton_c(c_query) }; // => calling C-wrapper here
    let query_response: &CStr = unsafe { CStr::from_ptr(query_response) };
    let query_response: &str = query_response.to_str().unwrap();

    return query_response; // => ownership moved to function calling this one
}
