#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate msql_srv;
extern crate proxy_ffi;

use slog::Drain;
use msql_srv::*;
use proxy_ffi::*;
use std::io;
use std::net;
use std::ffi::CString;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct Backend {
    rust_conn: FFIConnection,
    runtime: std::time::Duration,
    log: slog::Logger,
}

impl<W: io::Write> MysqlShim<W> for Backend {
    type Error = io::Error;

    // called when client issues request to prepare query for later execution
    fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        debug!(self.log, "Rust proxy: starting on_prepare");
        info.reply(42, &[], &[])
    }

    // called when client executes previously prepared statement
    fn on_execute(
        &mut self,
        _: u32,
        // any params included with the client's command:
        _: ParamParser,
        // response to query given using QueryResultWriter:
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        debug!(self.log, "Rust proxy: starting on_execute");
        results.completed(0, 0)
    }

    // called when client wants to deallocate resources associated with a prev prepared statement
    fn on_close(&mut self, _: u32) {
        debug!(self.log, "Rust proxy: starting on_close");
    }

    // called when client switches databases
    fn on_init(&mut self, _: &str, writer: InitWriter<W>) -> io::Result<()> {
        // first statement in sql would be `use db` => acknowledgement
        debug!(self.log, "Rust proxy: starting on_init");
        // Tell client that database context has been changed
        writer.ok()
    }

    // called when client issues query for immediate execution. Results returned via QueryResultWriter
    fn on_query(&mut self, q_string: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        // start measuring runtime
        // let start = Instant::now();

        debug!(self.log, "Rust proxy: starting on_query");
        debug!(self.log, "Rust Proxy: query received is: {:?}", q_string);

        // determine query type and return appropriate response
        if q_string.contains("CREATE TABLE") || q_string.contains("CREATE INDEX") || q_string.contains("CREATE VIEW") {
            let ddl_response = exec_ddl(&mut self.rust_conn, q_string);

            // stop measuring runtime and add to total time for this connection
            // let query_time = start.elapsed();
            // self.runtime = self.runtime + query_time;

            if ddl_response {
                results.completed(0, 0)
            } else {
                debug!(self.log, "Rust Proxy: Failed to execute CREATE");
                results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
            }
        } else if q_string.contains("UPDATE")
            || q_string.contains("DELETE")
            || q_string.contains("INSERT")
        {
            let update_response = exec_update(&mut self.rust_conn, q_string);

            // stop measuring runtime and add to total time for this connection 
            // let query_time = start.elapsed();
            // self.runtime = self.runtime + query_time;

            if update_response != -1 {
                results.completed(update_response as u64, 0)
            } else {
                debug!(self.log, "Rust Proxy: Failed to execute UPDATE");
                results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
            }
        } else if q_string.contains("SELECT") {
            let select_response = exec_select(&mut self.rust_conn, q_string);

            // stop measuring runtime and add to total time for this connection
            // let query_time = start.elapsed();
            // self.runtime = self.runtime + query_time;

            let num_cols = unsafe { (*select_response).num_cols as usize };
            let num_rows = unsafe { (*select_response).num_rows as usize };
            let col_names = unsafe { (*select_response).col_names };
            let col_types = unsafe { (*select_response).col_types };

            debug!(self.log, "Rust Proxy: converting response schema to rust compatible types");
            let cols = convert_columns(num_cols, col_types, col_names);
            let mut rw = results.start(&cols)?;

            debug!(self.log, "Rust Proxy: writing query response using RowWriter");
            // convert incomplete array field (flexible array) to rust slice, starting with the outer array
            let rows_slice = unsafe { (*select_response).values.as_slice(num_rows * num_cols) };
            for r in 0..num_rows {
                for c in 0..num_cols {
                    match col_types[c] {
                        // write a value to the next col of the current row (of this resultset)
                        FFIColumnType_UINT => unsafe {
                            rw.write_col(rows_slice[r * num_cols + c].UINT)?
                        },
                        FFIColumnType_INT => unsafe {
                            rw.write_col(rows_slice[r * num_cols + c].INT)?
                        },
                        FFIColumnType_TEXT => unsafe {
                            rw.write_col(
                                CString::from_raw(rows_slice[r * num_cols + c].TEXT)
                                    .into_string()
                                    .unwrap(),
                            )?
                        },
                        FFIColumnType_DATETIME => unsafe {
                            rw.write_col(
                                CString::from_raw(rows_slice[r * num_cols + c].DATETIME)
                                    .into_string()
                                    .unwrap(),
                            )?
                        },
                        _ => debug!(self.log, "Rust Proxy: Invalid column type"),
                    }
                }
                rw.end_row()?;
            }
            // call destructor for CResult (after results are written/copied via row writer)
            unsafe { std::ptr::drop_in_place(select_response) };

            // tell client no more rows coming. Returns an empty Ok to the proxy
            rw.finish()
        } else {
            debug!(self.log, "Rust proxy: unsupported query type");
            results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
        }
    }
}

// destructor to close connection when Backend goes out of scope
impl Drop for Backend {
    fn drop(&mut self) {
        debug!(self.log, "Rust Proxy: Starting destructor for Backend");
        debug!(self.log, "Rust Proxy: Calling c-wrapper for pelton::close\n");
        debug!(self.log, "Total time elapsed for this connection is: {:?}", self.runtime);
        let close_response: bool = close(&mut self.rust_conn);
        if close_response {
            debug!(self.log, "Rust Proxy: successfully closed connection");
        } else {
            debug!(self.log, "Rust Proxy: failed to close connection");
        }
    }
}

fn main() {
    // initialize logging
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log : slog::Logger = slog::Logger::root( slog_term::FullFormat::new(plain).build().fuse(), o!());

    let listener = net::TcpListener::bind("127.0.0.1:10001").unwrap();
    info!(log, "Listening at: {:?}", listener);

    let join_handle = std::thread::spawn(move || {
        let log = log.clone();
        if let Ok((stream, _addr)) = listener.accept() {
            info!(log,
                "Successfully connected to mysql proxy\nStream and address are: {:?}",
                stream
            );
            debug!(log, "Rust Proxy: calling c-wrapper for pelton::open\n");
            let rust_conn = open("", "root", "password"); 
            info!(log,
                "Rust Proxy: connection status is: {:?}",
                rust_conn.connected
            );
            let backend = Backend { rust_conn : rust_conn, runtime : Duration::new(0, 0), log : log };
            let _inter = MysqlIntermediary::run_on_tcp(backend, stream).unwrap();            
        }
    });
    join_handle.join().unwrap();
}
