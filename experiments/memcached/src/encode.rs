// Ported from memcached/encode.h.
use mysql::consts::ColumnType;
use mysql::{Column, Row};

pub type MemcachedKey = String;
pub type MemcachedRecord = String;

// Encode values. C++ overloaded EncodeValue() on the argument type; Rust has
// no overloading, so each overload becomes its own function.
pub fn encode_value_u64(val: u64) -> String {
    let mut v = val.to_string();
    while v.len() < std::mem::size_of::<u64>() {
        v = format!("0{}", v);
    }
    v
}

pub fn encode_value_i64(val: i64) -> String {
    let mut v = (if val < 0 { -val } else { val }).to_string();
    while v.len() < std::mem::size_of::<u64>() {
        v = format!("0{}", v);
    }
    if val < 0 {
        v = format!("-{}", v);
    }
    v
}

pub fn encode_value_str(val: String) -> String {
    val
}

// Encode a mariadb column. Indices are 1-based to match the JDBC
// ResultSetMetaData convention the original C++ used.
pub fn encode_column(row: &Row, columns: &[Column], i: usize) -> String {
    if i == 0 {
        return String::new();
    }
    let idx = i - 1;
    match columns[idx].column_type() {
        ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_TIMESTAMP
        | ColumnType::MYSQL_TYPE_TIMESTAMP2
        | ColumnType::MYSQL_TYPE_DATETIME
        | ColumnType::MYSQL_TYPE_DATETIME2 => {
            encode_value_str(row.get::<Option<String>, usize>(idx).unwrap().unwrap_or_default())
        }
        ColumnType::MYSQL_TYPE_TINY
        | ColumnType::MYSQL_TYPE_SHORT
        | ColumnType::MYSQL_TYPE_LONG
        | ColumnType::MYSQL_TYPE_LONGLONG
        | ColumnType::MYSQL_TYPE_INT24
        | ColumnType::MYSQL_TYPE_BIT
        | ColumnType::MYSQL_TYPE_DECIMAL
        | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            encode_value_i64(row.get::<Option<i64>, usize>(idx).unwrap().unwrap_or(0))
        }
        other => {
            panic!("Unknown mariadb type {:?}", other);
        }
    }
}

// Encode a base key.
pub fn encode_base_key(row: &Row, columns: &[Column], key_indices: &[usize]) -> MemcachedKey {
    let mut key = String::from("|");
    for &index in key_indices {
        key.push_str(&encode_column(row, columns, index));
        key.push('|');
    }
    key
}

// Encode a complete key.
pub fn encode_key(query: u64, key: &str, i: u64) -> MemcachedKey {
    format!(
        "{}@{}@{}",
        encode_value_u64(query),
        key,
        encode_value_u64(i)
    )
}

// Encode a complete row.
pub fn encode_row(row: &Row, columns: &[Column]) -> MemcachedRecord {
    let mut record = String::from("|");
    for i in 1..=columns.len() {
        record.push_str(&encode_column(row, columns, i));
        record.push('|');
    }
    record
}
