#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[macro_use]
extern crate slog;
extern crate ctrlc;
extern crate msql_srv;
extern crate proxy_ffi;
extern crate slog_term;

use msql_srv::*;
use proxy_ffi::*;
use slog::Drain;
use std::ffi::CString;
use std::io;
use std::net;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const USAGE: &str = "
  Available options:
  --help (false): displays this help message.
  --workers (3): number of dataflow workers.
  --db_name (pelton): name of the database.
  --db_username (root): database user to connect with to mariadb.
  --db_password (password): password to connect with to mariadb.";

#[derive(Debug)]
struct Backend {
    rust_conn: FFIConnection,
    runtime: std::time::Duration,
    log: slog::Logger,
    stop: Arc<AtomicBool>,
}

impl<W: io::Write> MysqlShim<W> for Backend {
    type Error = io::Error;

    // called when client issues request to prepare query for later execution
    fn on_prepare(&mut self, _q_string: &str, _info: StatementMetaWriter<W>) -> io::Result<()> {
        debug!(self.log, "Rust proxy: starting on_prepare");
        unimplemented!();
    }

    // called when client executes previously prepared statement
    fn on_execute(
        &mut self,
        _: u32,
        // any params included with the client's command:
        _: ParamParser,
        // response to query given using QueryResultWriter:
        _results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        debug!(self.log, "Rust proxy: starting on_execute");
        unimplemented!();
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
        // TODO(babman, benji): Figure out the DB name from the `use db`
        // statement and pass that to pelton.
        writer.ok()
    }

    // called when client issues query for immediate execution. Results returned via QueryResultWriter
    fn on_query(&mut self, q_string: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        // start measuring runtime
        let start = Instant::now();

        debug!(self.log, "Rust proxy: starting on_query");
        debug!(self.log, "Rust Proxy: query received is: {:?}", q_string);

        if q_string.starts_with("SET STOP") {
            self.stop.store(true, Ordering::Relaxed);
            results.completed(0, 0)
        } else if q_string.starts_with("SHOW VARIABLE") {
            // Hardcoded SHOW variables needed for mariadb java connectors.
            debug!(self.log, "Rust Proxy: SHOW statement simulated {}", q_string);

            let mut cols : Vec<Column> = Vec::with_capacity(2);
            cols.push(Column {
                table: "".to_string(),
                column: "Variable_name".to_string(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            });
            cols.push(Column {
                table: "".to_string(),
                column: "Value".to_string(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            });
            let mut rw = results.start(&cols)?;
            rw.write_col("auto_increment_increment")?;
            rw.write_col("1")?;
            rw.end_row()?;
            rw.write_col("max_allowed_packet")?;
            rw.write_col("16777216")?;
            rw.end_row()?;
            rw.write_col("system_time_zone")?;
            rw.write_col("EST")?;
            rw.end_row()?;
            rw.write_col("time_zone")?;
            rw.write_col("SYSTEM")?;
            rw.end_row()?;
            return rw.finish();
        } else if q_string.starts_with("CREATE TABLE")
                  || q_string.starts_with("CREATE INDEX")
                  || q_string.starts_with("CREATE VIEW")
                  || q_string.starts_with("SET") {
            let ddl_response = exec_ddl_ffi(&mut self.rust_conn, q_string);
            debug!(self.log, "ddl_response is {:?}", ddl_response);

            // stop measuring runtime and add to total time for this connection
            self.runtime = self.runtime + start.elapsed();

            if ddl_response {
                results.completed(0, 0)
            } else {
                error!(self.log, "Rust Proxy: Failed to execute CREATE");
                results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
            }
        } else if q_string.starts_with("UPDATE")
                  || q_string.starts_with("DELETE")
                  || q_string.starts_with("INSERT")
                  || q_string.starts_with("GDPR FORGET") {
            let update_response = exec_update_ffi(&mut self.rust_conn, q_string);

            // stop measuring runtime and add to total time for this connection
            self.runtime = self.runtime + start.elapsed();

            if update_response != -1 {
                results.completed(update_response as u64, 0)
            } else {
                error!(self.log, "Rust Proxy: Failed to execute UPDATE: {:?}", q_string);
                results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
            }
        } else if q_string.starts_with("SELECT")
                  || q_string.starts_with("GDPR GET")
                  || q_string.starts_with("SHOW SHARDS")
                  || q_string.starts_with("SHOW VIEW")
                  || q_string.starts_with("SHOW MEMORY") {
            let select_response = exec_select_ffi(&mut self.rust_conn, q_string);

            // stop measuring runtime and add to total time for this connection
            self.runtime = self.runtime + start.elapsed();

            let num_cols = unsafe { (*select_response).num_cols as usize };
            let num_rows = unsafe { (*select_response).num_rows as usize };
            let col_names = unsafe { (*select_response).col_names };
            let col_types = unsafe { (*select_response).col_types };

            debug!(
                self.log,
                "Rust Proxy: converting response schema to rust compatible types"
            );
            let cols = convert_columns(num_cols, col_types, col_names);
            let mut rw = results.start(&cols)?;

            debug!(
                self.log,
                "Rust Proxy: writing query response using RowWriter"
            );
            // convert incomplete array field (flexible array) to rust slice, starting with the outer array
            let rows_slice = unsafe { (*select_response).records.as_slice(num_rows * num_cols) };
            for r in 0..num_rows {
                for c in 0..num_cols {
                    if rows_slice[r * num_cols + c].is_null {
                        rw.write_col(None::<i32>)?
                    } else {
                        match col_types[c] {
                            // write a value to the next col of the current row (of this resultset)
                            FFIColumnType_UINT => unsafe {
                                rw.write_col(rows_slice[r * num_cols + c].record.UINT)?
                            },
                            FFIColumnType_INT => unsafe {
                                rw.write_col(rows_slice[r * num_cols + c].record.INT)?
                            },
                            FFIColumnType_TEXT => unsafe {
                                rw.write_col(
                                    CString::from_raw(rows_slice[r * num_cols + c].record.TEXT)
                                        .into_string()
                                        .unwrap(),
                                )?
                            },
                            FFIColumnType_DATETIME => unsafe {
                                rw.write_col(
                                    CString::from_raw(rows_slice[r * num_cols + c].record.DATETIME)
                                        .into_string()
                                        .unwrap(),
                                )?
                            },
                            _ => error!(self.log, "Rust Proxy: Invalid column type"),
                        }
                    }
                }
                rw.end_row()?;
            }
            // call destructor for CResult (after results are written/copied via row writer)
            unsafe { std::ptr::drop_in_place(select_response) };

            // tell client no more rows coming. Returns an empty Ok to the proxy
            rw.finish()
        } else {
            error!(self.log, "Rust proxy: unsupported query type {}", q_string);
            results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
        }
    }
}

// destructor to close connection when Backend goes out of scope
impl Drop for Backend {
    fn drop(&mut self) {
        debug!(self.log, "Rust Proxy: Starting destructor for Backend");
        debug!(self.log, "Rust Proxy: Calling c-wrapper for pelton::close");
        info!(
            self.log,
            "Total time elapsed for this connection is: {:?}", self.runtime
        );
        let close_response: bool = close_ffi(&mut self.rust_conn);
        if close_response {
            debug!(self.log, "Rust Proxy: successfully closed connection");
        } else {
            debug!(self.log, "Rust Proxy: failed to close connection");
        }
    }
}

fn main() {
    // initialize rust logging
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log: slog::Logger =
        slog::Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!());

    // Parse command line flags defined using rust's gflags.
    let flags = gflags_ffi(std::env::args(), USAGE);
    info!(log, "Rust proxy: running with args: {:?} {:?} {:?} {:?}",
          flags.workers, flags.db_name, flags.db_username, flags.db_password);

    let global_open = initialize_ffi(flags.workers);
    info!(log, "Rust Proxy: opening connection globally: {:?}", global_open);

    let listener = net::TcpListener::bind("127.0.0.1:10001").unwrap();
    info!(log, "Rust Proxy: Listening at: {:?}", listener);
    // set listener to non-blocking mode
    listener
        .set_nonblocking(true)
        .expect("Cannot set non-blocking");
    let stop = Arc::new(AtomicBool::new(false));

    // store client thread handles
    let mut threads = Vec::new();

    // detect listener status via shared boolean
    let stop_clone = Arc::clone(&stop);
    // detect SIGTERM to close listener
    let log_ctrlc = log.clone();
    ctrlc::set_handler(move || {
        info!(
            log_ctrlc,
            "Rust Proxy: received SIGTERM! Terminating listener once all client connections have closed..."
        );
        stop_clone.store(true, Ordering::Relaxed);
        debug!(
            log_ctrlc,
            "Rust Proxy: listener_terminated"
        );
    })
    .expect("Error setting Ctrl-C handler");

    // run listener until terminated with SIGTERM
    while !stop.load(Ordering::Relaxed) {
        while let Ok((stream, _addr)) = listener.accept() {
            let db_name = flags.db_name.clone();
            let db_username = flags.db_username.clone();
            let db_password = flags.db_password.clone();
            // clone log so that each client thread has an owned copy
            let log_client = log.clone();
            let stop_clone = stop.clone();
            threads.push(std::thread::spawn(move || {
                info!(
                    log_client,
                    "Rust Proxy: Successfully connected to mysql proxy\nStream and address are: {:?}",
                    stream
                );
                let rust_conn = open_ffi(&db_name, &db_username, &db_password);
                info!(
                    log_client,
                    "Rust Proxy: connection status is: {:?}", rust_conn.connected
                );
                let backend = Backend {
                    rust_conn: rust_conn,
                    runtime: Duration::new(0, 0),
                    log: log_client,
                    stop: stop_clone,
                };
                let _inter = MysqlIntermediary::run_on_tcp(backend, stream).unwrap();
            }));
        }
        // wait before checking listener status
        std::thread::sleep(Duration::from_secs(1));
    }

    // join all client threads
    for join_handle in threads {
        join_handle.join().unwrap();
    }
    let global_close = shutdown_ffi();
    info!(
        log,
        "Rust Proxy: Shutting down pelton, clearing global state: {:?}", global_close
    );
}
