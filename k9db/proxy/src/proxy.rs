#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[macro_use]
extern crate slog;
extern crate ffi;
extern crate lazy_static;
extern crate msql_srv;
extern crate slog_term;

use chrono::naive::NaiveDateTime;
use ffi::k9db;
use msql_srv::*;
use slog::Drain;
use std::io;
use std::io::Write;
use std::net;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

// Help message.
const USAGE: &str =
  "K9db options: look below for 'Flags from k9db/proxy/src/ffi/ffi.cc'";

// Helper for translating k9db types to msql-srv types.
fn convert_type(coltype: k9db::FFIColumnType) -> msql_srv::ColumnType {
  match coltype {
    k9db::FFIColumnType_UINT => ColumnType::MYSQL_TYPE_LONGLONG,
    k9db::FFIColumnType_INT => ColumnType::MYSQL_TYPE_LONGLONG,
    k9db::FFIColumnType_TEXT => ColumnType::MYSQL_TYPE_VAR_STRING,
    k9db::FFIColumnType_DATETIME => ColumnType::MYSQL_TYPE_DATETIME,
    _ => ColumnType::MYSQL_TYPE_NULL,
  }
}

// Helper for translating k9db schema into msql-srv columns.
fn convert_columns(result: k9db::FFIQueryResult) -> Vec<msql_srv::Column> {
  let num = k9db::result::column_count(result);
  let mut cols = Vec::with_capacity(num);
  for c in 0..num {
    let slice = k9db::result::column_name(result, c);
    let coltype = k9db::result::column_type(result, c);
    cols.push(Column { table: "".to_string(),
                       column: slice.to_string(),
                       coltype: convert_type(coltype),
                       colflags: ColumnFlags::empty() });
  }
  return cols;
}

// Helper for transforming msql-srv parameter to string.
fn stringify_parameter(param: msql_srv::ParamValue) -> String {
  if param.value.is_null() {
    return "NULL".to_string();
  }
  match param.coltype {
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
    ColumnType::MYSQL_TYPE_DATETIME => {
      let datetime: NaiveDateTime = param.value.into();
      datetime.format("'%Y-%m-%d %H:%M:%S%.f'").to_string()
    }
    _ => unimplemented!("Rust proxy: unsupported parameter type"),
  }
}

// Helper for writing a k9db resultset (for a SELECT) to msql-srv.
fn write_result<W: io::Write>(writer: msql_srv::QueryResultWriter<W>,
                              result: k9db::FFIQueryResult)
                              -> io::Result<()> {
  let mut cv: Vec<Vec<msql_srv::Column>> = Vec::new();
  while k9db::result::next_resultset(result) {
    cv.push(convert_columns(result));
  }

  let mut writer = writer;
  let mut i = 0;
  while k9db::result::next_resultset(result) {
    i += 1;
    let columns = &cv[i - 1];
    let mut rw = writer.start(columns).unwrap();

    let rows = k9db::result::row_count(result);
    let cols = columns.len();
    for r in 0..rows {
      for c in 0..cols {
        if k9db::result::null(result, r, c) {
          rw.write_col(None::<i32>)?;
          continue;
        }
        match k9db::result::column_type(result, c) {
          k9db::FFIColumnType_UINT => {
            rw.write_col(k9db::result::uint(result, r, c) as i64)
          }
          k9db::FFIColumnType_INT => {
            rw.write_col(k9db::result::int(result, r, c))
          }
          k9db::FFIColumnType_TEXT => {
            rw.write_col(k9db::result::text(result, r, c))
          }
          k9db::FFIColumnType_DATETIME => {
            rw.write_col(k9db::result::datetime(result, r, c))
          }
          _ => unimplemented!("Rust proxy: invalid column type"),
        }?;
      }
      rw.end_row()?;
    }
    writer = rw.finish_one()?;
  }

  k9db::result::destroy(result);
  writer.no_more_results()
}

// Write the result of an INSERT/UPDATE/DELETE.
fn write_update_result<W: io::Write>(writer: QueryResultWriter<W>,
                                     result: k9db::FFIUpdateResult)
                                     -> io::Result<()> {
  if result.row_count > -1 {
    return writer.completed(result.row_count as u64, result.last_insert_id);
  }
  return writer.error(ErrorKind::ER_INTERNAL_ERROR, &[2]);
}

// Write the given error through the writer.
macro_rules! write_error {
  ($writer:ident, $error:ident) => {
    $writer.error(ErrorKind::from($error.0), $error.1.as_bytes())
  };
}

// msql-srv shim struct.
#[derive(Debug)]
struct Backend {
  rust_conn: k9db::FFIConnection,
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
       && !stmt.starts_with("DELETE")
    {
      error!(self.log, "Rust Proxy: unsupported prepared statement {}", stmt);
      return info.error(ErrorKind::ER_SYNTAX_ERROR, stmt.as_bytes());
    }

    // Pass prepared statement to k9db for preparing.
    let result = match k9db::prepare(self.rust_conn, stmt) {
      Ok(result) => result,
      Err(e) => {
        error!(self.log,
               "Rust Proxy: cannot prepare statement {}.\n{}", stmt, e);
        return write_error!(info, e);
      }
    };

    // Return the number and type of parameters.
    let stmt_id = k9db::statement::id(result) as u32;
    let param_count = k9db::statement::arg_count(result);
    let mut params = Vec::with_capacity(param_count);
    for i in 0..param_count {
      let arg_type = match k9db::statement::arg_type(result, i) {
        Ok(result) => result,
        Err(e) => {
          error!(self.log,
                 "Rust Proxy: cannot prepare statement {}.\n{}", stmt, e);
          return write_error!(info, e);
        }
      };

      params.push(Column { table: "".to_string(),
                           column: "".to_string(),
                           colflags: ColumnFlags::empty(),
                           coltype: convert_type(arg_type) });
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
    let args: Vec<String> = params.into_iter()
                                  .map(|param| stringify_parameter(param))
                                  .collect();

    // Call k9db via ffi.
    let response = match k9db::exec_prepare(self.rust_conn, stmt_id, args) {
      Ok(response) => response,
      Err(e) => {
        error!(self.log,
               "Rust Proxy: cannot executed prepared statement.\n{}", e);
        return write_error!(results, e);
      }
    };

    // Response might be a query set or a response to an INSERT/UPDATE/DELETE.
    if response.query {
      return write_result(results, response.query_result);
    } else {
      return write_update_result(results, response.update_result);
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
       || q_string.starts_with("CREATE DATA_SUBJECT TABLE")
       || q_string.starts_with("CREATE INDEX")
       || q_string.starts_with("CREATE VIEW")
       || q_string.starts_with("SET")
       || q_string.starts_with("EXPLAIN COMPLIANCE")
       || q_string.starts_with("CTX")
    {
      let result = match k9db::exec_ddl(self.rust_conn, q_string) {
        Err(e) => {
          error!(self.log, "Rust Proxy: Error executing {}.\n{}", q_string, e);
          write_error!(results, e)
        }
        Ok(status) => {
          if status {
            results.completed(0, 0)
          } else {
            error!(self.log, "Rust Proxy: Failure executing {}", q_string);
            results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
          }
        }
      };
      return result;
    }

    // Data update statements.
    if q_string.starts_with("UPDATE")
       || q_string.starts_with("DELETE")
       || q_string.starts_with("INSERT")
       || q_string.starts_with("REPLACE")
       || q_string.starts_with("GDPR FORGET")
    {
      let result = match k9db::exec_update(self.rust_conn, q_string) {
        Err(e) => {
          error!(self.log, "Rust Proxy: Error executing {}.\n{}", q_string, e);
          write_error!(results, e)
        }
        Ok(update_result) => write_update_result(results, update_result),
      };
      return result;
    }

    // Queries.
    if q_string.starts_with("SELECT")
       || q_string.starts_with("GDPR GET")
       || q_string.starts_with("SHOW SHARDS")
       || q_string.starts_with("SHOW VIEW")
       || q_string.starts_with("SHOW MEMORY")
       || q_string.starts_with("SHOW PREPARED")
       || q_string.starts_with("SHOW PERF")
       || q_string.starts_with("SHOW INDICES")
       || q_string.starts_with("EXPLAIN")
    {
      let result = match k9db::exec_select(self.rust_conn, q_string) {
        Err(e) => {
          error!(self.log, "Rust Proxy: Error executing {}.\n{}", q_string, e);
          write_error!(results, e)
        }
        Ok(select_result) => write_result(results, select_result),
      };
      return result;
    }

    if q_string.starts_with("set") {
      info!(self.log, "Ignoring set query {}", q_string);
      return results.completed(0, 0);
    }

    if q_string.starts_with("STOP") {
      std::process::exit(0);
    }

    error!(self.log, "Rust Proxy: unsupported query type {}", q_string);
    results.error(ErrorKind::ER_SYNTAX_ERROR, q_string.as_bytes())
  }
}

// destructor to close connection when Backend goes out of scope
impl Drop for Backend {
  fn drop(&mut self) {
    info!(self.log, "Rust Proxy: Starting destructor for Backend");
    match k9db::close(self.rust_conn) {
      Ok(status) => {
        if status {
          info!(self.log, "Rust Proxy: successfully closed connection");
        } else {
          error!(self.log, "Rust Proxy: issue closing connection");
        }
      }
      Err(e) => {
        error!(self.log, "Rust Proxy: Error closing connection. {}", e);
      }
    }
  }
}

// Signals when the proxy should stop accepting new connections.
static stop: AtomicBool = AtomicBool::new(false);

#[no_mangle]
pub extern "C" fn proxy_terminate(sig: i32) {
  println!("Rust Proxy: Signal {}. Terminating after connections close..", sig);
  io::stdout().flush().unwrap();
  stop.store(true, Ordering::Relaxed);
}

fn main() {
  // initialize rust logging
  let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
  let log: slog::Logger =
    slog::Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!());

  // Parse command line flags defined using rust's gflags.
  let flags = match k9db::gflags(std::env::args(), USAGE) {
    Ok(flags) => flags,
    Err(e) => {
      error!(log, "Rust Proxy: Cannot parse cmd flags.\n{}", e);
      std::process::exit(-1);
    }
  };
  info!(log,
        "Rust proxy: running with args: {:?} {:?} {:?} {:?} {:?}",
        flags.workers,
        flags.consistent,
        flags.db_name,
        flags.hostname,
        flags.db_path);

  // Initialize K9db.
  match k9db::initialize(flags.workers,
                         flags.consistent,
                         &flags.db_name,
                         &flags.db_path)
  {
    Ok(status) => {
      if !status {
        error!(log, "Rust Proxy: cannot initialize K9db");
        std::process::exit(-1);
      }
    }
    Err(e) => {
      error!(log, "Rust Proxy: cannot initialize K9db.\n{}", e);
      std::process::exit(-1);
    }
  }

  // Listen for incoming connections.
  let listener = net::TcpListener::bind(flags.hostname).unwrap();
  info!(log, "Rust Proxy: Listening at: {:?}", listener);
  listener.set_nonblocking(true)
          .expect("Cannot set non-blocking");

  // store client thread handles
  let mut threads = Vec::new();

  // Run listener until terminated with SIGTERM
  while !stop.load(Ordering::Relaxed) {
    while let Ok((stream, _addr)) = listener.accept() {
      stream.set_nodelay(true).expect("Cannot disable nagle");
      // clone log so that each client thread has an owned copy
      let log = log.clone();
      threads.push(std::thread::spawn(move || {
                     let bufwriter =
                       io::BufWriter::with_capacity(10000000,
                                                    stream.try_clone()
                                                          .unwrap());
                     info!(log,
                           "Rust Proxy: Successfully connected to mysql \
                            proxy\nStream and address are: {:?}",
                           stream);
                     match k9db::open() {
                       Err(err) => error!(log,
                                          "Rust Proxy: Cannot open new \
                                           connection.\n{}",
                                          err),
                       Ok(rust_conn) => {
                         let backend = Backend { rust_conn: rust_conn,
                                                 log: log.clone() };
                         if let Err(e) =
                           MysqlIntermediary::run_on(backend, stream, bufwriter)
                         {
                           error!(log,
                                  "Rust Proxy: Cannot run backend via \
                                   MySqlIntermediary.\n{}",
                                  e);
                         }
                       }
                     }
                   }));
    }
    // wait before checking listener status
    std::thread::sleep(Duration::from_millis(1));
  }

  // join all client threads
  for join_handle in threads {
    join_handle.join().unwrap();
  }

  // Shutdown safetly.
  if let Err(e) = k9db::shutdown() {
    error!(log, "Rust Proxy: did not shutdown cleanly.\n{}", e);
  }
}
