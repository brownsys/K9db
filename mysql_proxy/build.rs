extern crate bindgen;

fn main() {
    // point to dir of compiled C libraries
    println!("cargo:rustc-link-search=src/cpp_wrapper");
    
    // tell cargo to invalidate built crate when C wrapper changes
    println!("cargo:rerun-if-changed=proxy.h");
}
