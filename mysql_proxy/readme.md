# Quickstart
Run the proxy from `mysql_proxy/src/cpp_wrapper` via `cargo run`
Connect to the specified address and port via an application or a mysql shell. 

# Summary
This proxy acts as a mysql database while interfacing with pelton. Queries are converted to C compatible types and sent to pelton. Responses are converted to rust compatible types and returned. Any application that uses a mysql backend may connect to this proxy instead of a traditional database.

- Coarse Grain
Application <=> Mysql Proxy <=> Pelton

- Fine Grain
Application <=> Mysql Proxy <=> FFI <=> C++ Wrappers <=> Pelton

# Design overview:
`mysql_proxy/src/main.rs` contains the code to start the proxy
`mysql_proxy/src/proxy_ffi.rs` contains the FFI interface with C
`mysql_proxy/src/proxy_wrappers.rs` contains functions which convert rust types to C, call the appropriate FFI functions into pelton, and return the response to the proxy
`mysql_proxy/src/cpp_wrapper/proxy.h` contains the C/C++ API for the rust proxy. Its functions convert C++ types to C and pass query responses to rust via the FFI.  
