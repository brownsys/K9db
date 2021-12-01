#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[macro_use]
extern crate slog;
extern crate ctrlc;
extern crate msql_srv;
extern crate proxy_ffi;
extern crate regex;
extern crate slog_term;

use msql_srv::*;
use proxy_ffi::*;
use regex::Regex;
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
    prepared_statements: Vec<Vec<String>>,
}

impl<W: io::Write> MysqlShim<W> for Backend {
    type Error = io::Error;

    // called when client issues request to prepare query for later execution
    fn on_prepare(
        &mut self,
        prepared_statement: &str,
        info: StatementMetaWriter<W>,
    ) -> io::Result<()> {
        debug!(self.log, "Rust proxy: starting on_prepare");
        debug!(
            self.log,
            "Rust Proxy: prepared statement received is: {:?}", prepared_statement
        );

        let statement_id = self.prepared_statements.len() as u32;
        if prepared_statement.starts_with("SELECT") {
            let view_name = String::from("q") + &statement_id.to_string();

            // Convert a statement of the form of "SELECT * FROM table WHERE col=?"
            // to "CREATE VIEW q0 A '"SELECT * FROM table WHERE col=?"'" to be
            // passed to Pelton
            let mut create_view_stmt = String::from("CREATE VIEW ");
            create_view_stmt.push_str(&view_name);
            create_view_stmt.push_str(" AS '\"");
            create_view_stmt.push_str(&prepared_statement);
            create_view_stmt.push_str("\"';");

            // Execute create view statement in Pelton
            if !exec_ddl_ffi(&mut self.rust_conn, &create_view_stmt) {
                return info.error(ErrorKind::ER_INTERNAL_ERROR, &[2]);
            }

            // Change query from "SELECT * FROM table WHERE col=?" to
            // "SELECT * FROM q0 WHERE col=?", where q0 is the view name
            // assigned to the prepared statement, makes parameterizing and
            // executing prepared statement later easier.
            let re = Regex::new(r"(?P<expr>\s[A-Za-z_0-9`]+\s*\(=|>|<|>=|<=\)\s*)[?]").unwrap();
            let mut view_query: Vec<String> = re
                .captures_iter(prepared_statement)
                .map(|caps| caps["expr"].to_string())
                .collect();
            if view_query.len() == 0 {
                view_query.push(String::from("SELECT * FROM ") + &view_name + ";");
            } else {
                view_query[0] = String::from("SELECT * FROM ") + &view_name + " " + &view_query[0];
                view_query.push(String::from(";"));
            }

            // Used to store the column structs representing the parameters to
            // be returned to the MySQL client
            let mut params = Vec::new();
            for _i in 1..view_query.len() {
                params.push(Column {
                    table: "".to_string(),
                    column: "".to_string(),
                    coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                    colflags: ColumnFlags::empty(),
                });
            }

            debug!(self.log, "Rust proxy: Prepared as: {:?}", view_query);
            self.prepared_statements.push(view_query);

            // Respond to client
            info.reply(statement_id, &params, &[])
        } else if prepared_statement.starts_with("INSERT") {
            // What the prepared statement components are.
            // This is saved for future use in on_execute.
            let components: Vec<String> = prepared_statement
                .split("?")
                .map(|s| s.to_string())
                .collect();

            // Used to store the column structs representing the parameters to
            // be returned to the MySQL client
            let mut params = Vec::new();
            for _i in 1..components.len() {
                params.push(Column {
                    table: "".to_string(),
                    column: "".to_string(),
                    coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                    colflags: ColumnFlags::empty(),
                });
            }

            debug!(self.log, "Rust proxy: Prepared as: {:?}", components);
            self.prepared_statements.push(components);

            // Respond to client
            info.reply(statement_id, &params, &[])
        } else {
            error!(self.log, "Rust proxy: unsupported prepared statement type");
            info.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
        }
    }

    // called when client executes previously prepared statement
    // params: any params included with the client's command:
    // response to query given using QueryResultWriter.
    fn on_execute(
        &mut self,
        stmt_id: u32,
        params: ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        debug!(self.log, "Rust proxy: starting on_execute");

        let mut query: String = String::new();
        let tokens = &self.prepared_statements[stmt_id as usize];

        // Add parameters passed to MySQL client to the prepared statement
        let mut i = 0;
        for param in params {
            query.push_str(&tokens[i]);
            let val = match param.coltype {
                // If string, surround with \"
                ColumnType::MYSQL_TYPE_STRING
                | ColumnType::MYSQL_TYPE_VARCHAR
                | ColumnType::MYSQL_TYPE_VAR_STRING => {
                    format!("\'{}\'", Into::<&str>::into(param.value))
                }
                ColumnType::MYSQL_TYPE_DECIMAL
                | ColumnType::MYSQL_TYPE_NEWDECIMAL
                | ColumnType::MYSQL_TYPE_TINY
                | ColumnType::MYSQL_TYPE_SHORT
                | ColumnType::MYSQL_TYPE_INT24
                | ColumnType::MYSQL_TYPE_LONG
                | ColumnType::MYSQL_TYPE_LONGLONG => Into::<i64>::into(param.value).to_string(),
                _ => unimplemented!("Rust proxy: unsupported parameter type"),
            };
            query.push_str(&val);
            i += 1;
        }
        query.push_str(tokens.last().unwrap());

        // Execute modified prepared statement like new query
        debug!(self.log, "Rust proxy: Executing as: {:?}", query);
        self.on_query(&query, results)
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
        } else if q_string.starts_with("SHOW VARIABLES") {
            // Hardcoded SHOW variables needed for mariadb java connectors.
            debug!(
                self.log,
                "Rust Proxy: SHOW statement simulated {}", q_string
            );

            let mut cols: Vec<Column> = Vec::with_capacity(2);
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
            || q_string.starts_with("SET")
        {
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
            || q_string.starts_with("GDPR FORGET")
        {
            let update_response = exec_update_ffi(&mut self.rust_conn, q_string);

            // stop measuring runtime and add to total time for this connection
            self.runtime = self.runtime + start.elapsed();

            if update_response != -1 {
                results.completed(update_response as u64, 0)
            } else {
                error!(
                    self.log,
                    "Rust Proxy: Failed to execute UPDATE: {:?}", q_string
                );
                results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
            }
        } else if q_string.starts_with("SELECT")
            || q_string.starts_with("GDPR GET")
            || q_string.starts_with("SHOW SHARDS")
            || q_string.starts_with("SHOW VIEW")
            || q_string.starts_with("SHOW MEMORY")
        {
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
                                rw.write_col(rows_slice[r * num_cols + c].record.UINT as i64)?
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
        } else if q_string.starts_with("set") {
            debug!(self.log, "Ignoring set query {}", q_string);
            results.completed(0, 0)
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
    info!(
        log,
        "Rust proxy: running with args: {:?} {:?} {:?} {:?}",
        flags.workers,
        flags.db_name,
        flags.db_username,
        flags.db_password
    );

    let global_open = initialize_ffi(flags.workers);
    info!(
        log,
        "Rust Proxy: opening connection globally: {:?}", global_open
    );

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
                    prepared_statements: Vec::new(),
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
