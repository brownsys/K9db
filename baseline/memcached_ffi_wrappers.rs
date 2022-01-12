#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

pub mod _memcached_ffi_bindgen {
  include!("memcached_ffi_bindgen.rs");
}

pub mod memcached {
  use std::slice;
  use std::ffi::{CStr, CString};
  use std::fmt::Write;
  use std::mem;

  use crate::_memcached_ffi_bindgen::{Type_UINT, Type_INT, Type_TEXT, Value, Record};
  use crate::_memcached_ffi_bindgen::DestroyResult;
  use crate::_memcached_ffi_bindgen::{SetStr, SetInt, SetUInt, GetStr, GetUInt, GetInt};
  use crate::_memcached_ffi_bindgen::{Initialize, Cache, Update, Read, __ExecuteDB, Close};

  // Initializing.
  pub fn MemInitialize(db: &str, db_user: &str, db_pass: &str, seed: &str) -> bool {
    let c_db = CString::new(db).unwrap();
    let c_db_user = CString::new(db_user).unwrap();
    let c_db_pass = CString::new(db_pass).unwrap();
    let c_seed = CString::new(seed).unwrap();
    unsafe { Initialize(c_db.as_ptr(), c_db_user.as_ptr(), c_db_pass.as_ptr(), c_seed.as_ptr()) }
  }

  // Creating a value.
  pub fn MemSetStr(v: &str) -> Value {
    let cstring = CString::new(v).unwrap();
    unsafe { SetStr(cstring.as_ptr()) }
  }

  pub fn MemSetInt(v: i64) -> Value {
    unsafe { SetInt(v) }
  }

  pub fn MemSetUInt(v: u64) -> Value {
    unsafe { SetUInt(v) }
  }

  // Creating a key.
  pub fn MemCreateKey(mut values: Vec<Value>) -> Record {
    values.shrink_to_fit();
    let size = values.len();
    let ptr = values.as_mut_ptr();
    mem::forget(values);
    Record{size: size, values: ptr}
  }

  // Caching a query.
  pub fn MemCache(query: &str) -> i32 {
    let cstring = CString::new(query).unwrap();
    unsafe { Cache(cstring.as_ptr()) }
  }

  // Updating cache for table.
  pub fn MemUpdate(table: &str) {
    let cstring = CString::new(table).unwrap();
    unsafe { Update(cstring.as_ptr()) }
  }

  // Reading from cache at key.
  pub fn MemRead(id: i32, key: Record) -> Vec<String> {
    let mut vec = Vec::new();
    let mut result = unsafe { Read(id, &key) };
    let records = unsafe { slice::from_raw_parts(result.records, result.size) };
    for row in records {
      let values = unsafe { slice::from_raw_parts(row.values, row.size) };
      let mut row_str: String = String::from("|");
      for value in values {
        if value.type_ == Type_UINT {
          write!(row_str, "{}|", unsafe { GetUInt(value) }).unwrap();
        } else if value.type_ == Type_INT {
          write!(row_str, "{}|", unsafe { GetInt(value) }).unwrap();
        } else if value.type_ == Type_TEXT {
          write!(row_str, "{}|", unsafe { CStr::from_ptr(GetStr(value)).to_str().unwrap() }).unwrap();
        }
      }
      vec.push(row_str);
    }
    unsafe { DestroyResult(&mut result) }
    return vec;
  }

  pub fn __MemExecuteDB(stmt: &str) {
    let cstring = CString::new(stmt).unwrap();
    unsafe { __ExecuteDB(cstring.as_ptr()) }
  }
  
  pub fn MemClose() {
    unsafe { Close() }
  }
}
