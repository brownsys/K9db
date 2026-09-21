// Ported from memcached/mariadb.h.
use std::collections::HashMap;

use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder, Row};

use crate::encode::{encode_base_key, encode_row, MemcachedKey, MemcachedRecord};

pub struct MariaDBConnection {
    conn: Conn,
}

impl MariaDBConnection {
    pub fn new(host: &str) -> MariaDBConnection {
        // Connect to mariadb server.
        let opts = OptsBuilder::new()
            .ip_or_hostname(Some(host))
            .user(Some("k9db"))
            .pass(Some("password"));

        let mut conn = Conn::new(opts).expect("no driver");
        conn.query_drop("USE lobsters").unwrap();
        MariaDBConnection { conn }
    }

    pub fn query(
        &mut self,
        key_columns: &[String],
        q: &str,
        limit: i64,
    ) -> HashMap<MemcachedKey, Vec<MemcachedRecord>> {
        // Execute the query.
        let rows: Vec<Row> = self.conn.query(q).unwrap();

        // We will put the data here.
        let mut data: HashMap<MemcachedKey, Vec<MemcachedRecord>> = HashMap::new();
        if rows.is_empty() {
            return data;
        }

        // Unlike JDBC's ResultSetMetaData, mysql::Row carries its own column
        // metadata, so we read it off the first row instead of the query result.
        let columns = rows[0].columns();

        // Find the index of the key column in the result set.
        let mut key_indices = Vec::new();
        for key_column in key_columns {
            for i in 1..=columns.len() {
                if columns[i - 1].name_str() == key_column.as_str() {
                    key_indices.push(i);
                    break;
                }
            }
        }
        if key_indices.len() != key_columns.len() {
            panic!("Key column(s) missing from result");
        }

        // Read each row and put it in data with the correct key.
        for row in &rows {
            let key = encode_base_key(row, &columns, &key_indices);
            let record = encode_row(row, &columns);
            let entry = data.entry(key).or_insert_with(Vec::new);
            if limit == -1 || (entry.len() as i64) < limit {
                entry.push(record);
            }
        }
        data
    }
}
