#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[macro_use]
extern crate slog;
extern crate ctrlc;
extern crate lazy_static;
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
use std::time::Duration;

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
  log: slog::Logger,
}

impl<W: io::Write> MysqlShim<W> for Backend {
  type Error = io::Error;

  // called when client issues request to prepare query for later execution
  fn on_prepare(&mut self,
                stmt: &str,
                info: StatementMetaWriter<W>)
                -> io::Result<()> {
    // Filter out supported statements.
    if !stmt.starts_with("SELECT")
       && !stmt.starts_with("UPDATE")
       && !stmt.starts_with("INSERT")
       && !stmt.starts_with("REPLACE")
    {
      error!(self.log, "Rust proxy: unsupported prepared statement {}", stmt);
      return info.error(ErrorKind::ER_INTERNAL_ERROR, &[2]);
    }

    // Pass prepared statement to pelton for preparing.
    let result = prepare_ffi(&mut self.rust_conn, stmt);
    let stmt_id = unsafe { (*result).stmt_id } as u32;
    let param_count = unsafe { (*result).arg_count };
    let types = unsafe { (*result).args.as_slice(param_count) };

    // Return the number and type of parameters.
    let mut params = Vec::with_capacity(param_count);
    for i in 0..param_count {
      params.push(Column { table: "".to_string(),
                           column: "".to_string(),
                           colflags: ColumnFlags::empty(),
                           coltype: convert_column_type(types[i]) });
    }

    if param_count > 1000 {
      println!("Perf Warning: High Parameter Count {} in Query {}",
               param_count, stmt);
    }

    // Respond to client
    return info.reply(stmt_id, &params, &[]);
  }

  // called when client executes previously prepared statement
  // params: any params included with the client's command:
  // response to query given using QueryResultWriter.
  fn on_execute(&mut self,
                stmt_id: u32,
                params: ParamParser,
                results: QueryResultWriter<W>)
                -> io::Result<()> {
    // Push all parameters (in order) into args formatted by type.
    let mut args: Vec<String> = Vec::new();
    for param in params.into_iter() {
      if param.value.is_null() {
        args.push("NULL".to_string());
      } else {
        let val = match param.coltype {
          ColumnType::MYSQL_TYPE_STRING
          | ColumnType::MYSQL_TYPE_VARCHAR
          | ColumnType::MYSQL_TYPE_VAR_STRING => {
            format!("'{}'", Into::<&str>::into(param.value))
          }
          ColumnType::MYSQL_TYPE_DECIMAL
          | ColumnType::MYSQL_TYPE_NEWDECIMAL
          | ColumnType::MYSQL_TYPE_TINY
          | ColumnType::MYSQL_TYPE_SHORT
          | ColumnType::MYSQL_TYPE_INT24
          | ColumnType::MYSQL_TYPE_LONG
          | ColumnType::MYSQL_TYPE_LONGLONG => match param.value.into_inner() {
            ValueInner::Int(v) => v.to_string(),
            ValueInner::UInt(v) => v.to_string(),
            _ => unimplemented!("Rust proxy: unsupported numeric type"),
          },
          ColumnType::MYSQL_TYPE_DOUBLE | ColumnType::MYSQL_TYPE_FLOAT => {
            match param.value.into_inner() {
              ValueInner::Double(v) => (v.floor() as i64).to_string(),
              _ => unimplemented!("Rust proxy: unsupported double type"),
            }
          }
          _ => unimplemented!("Rust proxy: unsupported parameter type"),
        };
        args.push(val);
      }
    }

    // Call pelton via ffi.
    let response =
      exec_prepared_ffi(&mut self.rust_conn, stmt_id as usize, args);

    if response.query {
      let select_response = response.query_result;
      if !select_response.is_null() {
        let num_cols = unsafe { (*select_response).num_cols as usize };
        let num_rows = unsafe { (*select_response).num_rows as usize };
        let col_names = unsafe { (*select_response).col_names };
        let col_types = unsafe { (*select_response).col_types };

        let cols = convert_columns(num_cols, col_types, col_names);
        let mut rw = results.start(&cols)?;

        // convert incomplete array field (flexible array) to rust slice,
        // starting with the outer array
        let rows_slice =
          unsafe { (*select_response).records.as_slice(num_rows * num_cols) };
        for r in 0..num_rows {
          for c in 0..num_cols {
            if rows_slice[r * num_cols + c].is_null {
              rw.write_col(None::<i32>)?
            } else {
              match col_types[c] {
                // write a value to the next col of the current row (of this
                // resultset)
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
                    CString::from_raw(
                      rows_slice[r * num_cols + c].record.DATETIME,
                    )
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
        // call destructor for CResult (after results are written/copied via row
        // writer)
        unsafe { std::ptr::drop_in_place(select_response) };

        // tell client no more rows coming. Returns an empty Ok to the proxy
        return rw.finish();
      }
      error!(self.log, "Rust Proxy: Failed to exec prepared select");
      return results.error(ErrorKind::ER_INTERNAL_ERROR, &[2]);
    } else {
      let update_response = response.update_result;
      if update_response != -1 {
        return results.completed(update_response as u64, 0);
      }
      error!(self.log, "Rust Proxy: Failed to exec prepared update");
      return results.error(ErrorKind::ER_INTERNAL_ERROR, &[2]);
    }
  }

  // called when client wants to deallocate resources associated with a prev
  // prepared statement
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

  // called when client issues query for immediate execution. Results returned
  // via QueryResultWriter
  fn on_query(&mut self,
              q_string: &str,
              results: QueryResultWriter<W>)
              -> io::Result<()> {
    // Hardcoded SHOW variables needed for mariadb java connectors.
    if q_string.starts_with("SHOW VARIABLES") {
      info!(self.log, "Rust Proxy: SHOW statement simulated {}", q_string);

      let mut cols: Vec<Column> = Vec::with_capacity(2);
      cols.push(Column { table: "".to_string(),
                         column: "Variable_name".to_string(),
                         coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                         colflags: ColumnFlags::empty() });
      cols.push(Column { table: "".to_string(),
                         column: "Value".to_string(),
                         coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                         colflags: ColumnFlags::empty() });
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
    }

    // CREATE statements.
    if q_string.starts_with("CREATE TABLE")
       || q_string.starts_with("CREATE INDEX")
       || q_string.starts_with("CREATE VIEW")
       || q_string.starts_with("SET")
    {
      let ddl_response = exec_ddl_ffi(&mut self.rust_conn, q_string);
      if ddl_response {
        return results.completed(0, 0);
      }
      error!(self.log, "Rust Proxy: Failed to execute CREATE {:?}", q_string);
      return results.error(ErrorKind::ER_INTERNAL_ERROR, &[2]);
    }

    // Data update statements.
    if q_string.starts_with("UPDATE")
       || q_string.starts_with("DELETE")
       || q_string.starts_with("INSERT")
       || q_string.starts_with("REPLACE")
       || q_string.starts_with("GDPR FORGET")
    {
      let update_response = exec_update_ffi(&mut self.rust_conn, q_string);
      if update_response != -1 {
        return results.completed(update_response as u64, 0);
      }
      error!(self.log, "Rust Proxy: Failed to execute UPDATE: {:?}", q_string);
      return results.error(ErrorKind::ER_INTERNAL_ERROR, &[2]);
    }

    // Queries.
    if q_string.starts_with("SELECT")
       || q_string.starts_with("GDPR GET")
       || q_string.starts_with("SHOW SHARDS")
       || q_string.starts_with("SHOW VIEW")
       || q_string.starts_with("SHOW MEMORY")
       || q_string.starts_with("SHOW PREPARED")
    {
      let select_response = exec_select_ffi(&mut self.rust_conn, q_string);
      if !select_response.is_null() {
        let num_cols = unsafe { (*select_response).num_cols as usize };
        let num_rows = unsafe { (*select_response).num_rows as usize };
        let col_names = unsafe { (*select_response).col_names };
        let col_types = unsafe { (*select_response).col_types };

        let cols = convert_columns(num_cols, col_types, col_names);
        let mut rw = results.start(&cols)?;

        // convert incomplete array field (flexible array) to rust slice,
        // starting with the outer array
        let rows_slice =
          unsafe { (*select_response).records.as_slice(num_rows * num_cols) };
        for r in 0..num_rows {
          for c in 0..num_cols {
            if rows_slice[r * num_cols + c].is_null {
              rw.write_col(None::<i32>)?
            } else {
              match col_types[c] {
                // write a value to the next col of the current row (of this
                // resultset)
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
                    CString::from_raw(
                      rows_slice[r * num_cols + c].record.DATETIME,
                    )
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
        // call destructor for CResult (after results are written/copied via row
        // writer)
        unsafe { std::ptr::drop_in_place(select_response) };

        // tell client no more rows coming. Returns an empty Ok to the proxy
        return rw.finish();
      }
      error!(self.log, "Rust Proxy: Failed to execute SELECT: {:?}", q_string);
      return results.error(ErrorKind::ER_INTERNAL_ERROR, &[2]);
    }

    if q_string.starts_with("set") {
      info!(self.log, "Ignoring set query {}", q_string);
      return results.completed(0, 0);
    }

    error!(self.log, "Rust proxy: unsupported query type {}", q_string);
    results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
  }
}

// destructor to close connection when Backend goes out of scope
impl Drop for Backend {
  fn drop(&mut self) {
    info!(self.log, "Rust Proxy: Starting destructor for Backend");
    if close_ffi(&mut self.rust_conn) {
      info!(self.log, "Rust Proxy: successfully closed connection");
    } else {
      info!(self.log, "Rust Proxy: failed to close connection");
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
  info!(log,
        "Rust proxy: running with args: {:?} {:?} {:?} {:?}",
        flags.workers,
        flags.consistent,
        flags.db_name,
        flags.hostname);

  let global_open = initialize_ffi(flags.workers, flags.consistent);
  if !global_open {
    std::process::exit(-1);
  }

  let listener = net::TcpListener::bind(flags.hostname).unwrap();
  info!(log, "Rust Proxy: Listening at: {:?}", listener);
  listener.set_nonblocking(true)
          .expect("Cannot set non-blocking");
  let stop = Arc::new(AtomicBool::new(false));

  // store client thread handles
  let mut threads = Vec::new();

  // Detect stop via ctrl+c.
  let log_ctrlc = log.clone();
  let stop_ctrlc = stop.clone();
  ctrlc::set_handler(move || {
    info!(log_ctrlc,
          "Rust Proxy: SIGTERM! Terminating after connections close...");
    stop_ctrlc.store(true, Ordering::Relaxed);
  }).expect("Error setting Ctrl-C handler");

  // run listener until terminated with SIGTERM
  while !stop.load(Ordering::Relaxed) {
    while let Ok((stream, _addr)) = listener.accept() {
      let db_name = flags.db_name.clone();
      // clone log so that each client thread has an owned copy
      let log = log.clone();
      threads.push(std::thread::spawn(move || {
              info!(log,
                    "Rust Proxy: Successfully connected to mysql \
                     proxy\nStream and address are: {:?}",
                    stream);
              let rust_conn = open_ffi(&db_name);
              if rust_conn.connected {
                let backend = Backend { rust_conn: rust_conn,
                                        log: log };
                let _ = MysqlIntermediary::run_on_tcp(backend, stream).unwrap();
              }
            }));
    }
    // wait before checking listener status
    std::thread::sleep(Duration::from_secs(1));
  }

  // join all client threads
  for join_handle in threads {
    join_handle.join().unwrap();
  }
  shutdown_ffi();
}
