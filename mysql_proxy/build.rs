extern crate bindgen;

use std::env;
use std::path::PathBuf;

fn main() {
    // include!("src/cpp_wrapper/simple.h");
    println!("cargo:include=src/cpp_wrapper/simple.h");

    // point to dir of compiled C libraries
    println!("cargo:rustc-link-search=src/cpp_wrapper");

    // tell cargo to invalidate built crate when C wrapper changes
    // println!("cargo:rerun-if-changed=simple.h");

    let bindings = bindgen::Builder::default()
        .header("src/cpp_wrapper/simple.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
