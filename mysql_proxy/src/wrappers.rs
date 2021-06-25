use std::ffi::CString;
use std::ffi::CStr;
use std::os::raw::c_char;

include!("string_ffi.rs");
include!("open_ffi.rs");

// convert rust string to C, call C-wrapper, convert C++ response to rust string
fn send_string(s: &str) -> &str {

    // Convert to *mut i8 to send to C-function
    let c_query = CString::new(s).unwrap();
    let c_query: *mut c_char = c_query.as_ptr() as *mut i8;
    
    // Receive *mut i8 back from C and convert to rust string ('move' so rust gets ownership of the heap allocated string)
    let query_response : *mut c_char = unsafe {query_pelton_c(c_query)}; // => calling C-wrapper here
    let query_response : &CStr = unsafe {CStr::from_ptr(query_response)};
    let query_response : &str = query_response.to_str().unwrap();

    return query_response; // => ownership moved to function calling this one
}

fn open() -> bool {
  // Convert to *mut i8 to send to C-function
  let d = CString::new("").unwrap();
  let d: *mut c_char = d.as_ptr() as *mut i8;
  // Convert to *mut i8 to send to C-function
  let u = CString::new("root").unwrap();
  let u: *mut c_char = u.as_ptr() as *mut i8;
  // Convert to *mut i8 to send to C-function
  let p = CString::new("password").unwrap();
  let p: *mut c_char = p.as_ptr() as *mut i8;

  let conn = unsafe {create()};
  let result = unsafe {open_c(d, u, p, conn)};
  unsafe {destroy(conn)};
  return result;
}
