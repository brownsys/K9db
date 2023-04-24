#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[macro_use]
extern crate slog;
extern crate ffi;
extern crate lazy_static;
extern crate msql_srv;
extern crate slog_term;

use ffi::pelton;
use msql_srv::*;
use slog::Drain;
use std::io;
use std::io::Write;
use std::net;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

// Help message.
const USAGE: &str = "
  Available options:
  --help (false): displays this help message.
  --workers (3): number of dataflow workers.
  --db_name (pelton): name of the database.
  --db_username (root): database user to connect with to mariadb.
  --db_password (password): password to connect with to mariadb.";

// Helper for translating pelton types to msql-srv types.
fn convert_type(coltype: pelton::FFIColumnType) -> msql_srv::ColumnType {
  match coltype {
    pelton::FFIColumnType_UINT => ColumnType::MYSQL_TYPE_LONGLONG,
    pelton::FFIColumnType_INT => ColumnType::MYSQL_TYPE_LONGLONG,
    pelton::FFIColumnType_TEXT => ColumnType::MYSQL_TYPE_VAR_STRING,
    pelton::FFIColumnType_DATETIME => ColumnType::MYSQL_TYPE_VAR_STRING,
    _ => ColumnType::MYSQL_TYPE_NULL,
  }
}

// Helper for translating pelton schema into msql-srv columns.
fn convert_columns(result: pelton::FFIResult) -> Vec<msql_srv::Column> {
  let num = pelton::result::column_count(result);
  let mut cols = Vec::with_capacity(num);
  for c in 0..num {
    let slice = pelton::result::column_name(result, c);
    let coltype = pelton::result::column_type(result, c);
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
    _ => unimplemented!("Rust proxy: unsupported parameter type"),
  }
}

// Helper for writing a pelton result to msql-srv.
fn write_result<W: io::Write>(writer: msql_srv::QueryResultWriter<W>,
                              result: pelton::FFIResult)
                              -> io::Result<()> {
  let mut cv: Vec<Vec<msql_srv::Column>> = Vec::new();
  while pelton::result::next_resultset(result) {
    cv.push(convert_columns(result));
  }

  let mut writer = writer;
  let mut i = 0;
  while pelton::result::next_resultset(result) {
    i += 1;
    let columns = &cv[i-1];
    let mut rw = writer.start(columns).unwrap();

    let rows = pelton::result::row_count(result);
    let cols = columns.len();
    for r in 0..rows {
      for c in 0..cols {
        if pelton::result::null(result, r, c) {
          rw.write_col(None::<i32>)?;
          continue;
        }
        match pelton::result::column_type(result, c) {
          pelton::FFIColumnType_UINT => {
            rw.write_col(pelton::result::uint(result, r, c) as i64)
          }
          pelton::FFIColumnType_INT => {
            rw.write_col(pelton::result::int(result, r, c))
          }
          pelton::FFIColumnType_TEXT => {
            rw.write_col(pelton::result::text(result, r, c))
          }
          pelton::FFIColumnType_DATETIME => {
            rw.write_col(pelton::result::datetime(result, r, c))
          }
          _ => unimplemented!("Rust proxy: invalid column type"),
        }?;
      }
      rw.end_row()?;
    }
    writer = rw.finish_one()?;
  }

  pelton::result::destroy(result);
  writer.no_more_results()
}

// msql-srv shim struct.
#[derive(Debug)]
struct Backend {
  rust_conn: pelton::FFIConnection,
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
    let result = pelton::prepare(self.rust_conn, stmt);
    let stmt_id = pelton::statement::id(result) as u32;
    let param_count = pelton::statement::arg_count(result);

    // Return the number and type of parameters.
    let mut params = Vec::with_capacity(param_count);
    for i in 0..param_count {
      let arg_type = pelton::statement::arg_type(result, i);
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
    let mut args: Vec<String> = Vec::new();
    for param in params.into_iter() {
      args.push(stringify_parameter(param));
    }

    // Call pelton via ffi.
    let response = pelton::exec_prepare(self.rust_conn, stmt_id, args);
    if response.query {
      return write_result(results, response.query_result);
    } else {
      if response.update_result > -1 {
        return results.completed(response.update_result as u64,
                                 response.last_insert_id);
      }
    }
    return results.error(ErrorKind::ER_INTERNAL_ERROR, &[2]);
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
       || q_string.starts_with("CREATE DATA_SUBJECT TABLE")
       || q_string.starts_with("CREATE INDEX")
       || q_string.starts_with("CREATE VIEW")
       || q_string.starts_with("SET")
       || q_string.starts_with("EXPLAIN COMPLIANCE")
       || q_string.starts_with("CTX")
    {
      let ddl_response = pelton::exec_ddl(self.rust_conn, q_string);
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
      let update_response = pelton::exec_update(self.rust_conn, q_string);
      if update_response.row_count != -1 {
        return results.completed(update_response.row_count as u64,
                                 update_response.last_insert_id);
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
       || q_string.starts_with("SHOW PERF")
       || q_string.starts_with("SHOW INDICES")
       || q_string.starts_with("EXPLAIN")
    {
      let select_response = pelton::exec_select(self.rust_conn, q_string);
      return write_result(results, select_response);
    }

    if q_string.starts_with("set") {
      info!(self.log, "Ignoring set query {}", q_string);
      return results.completed(0, 0);
    }

    if q_string.starts_with("STOP") {
      std::process::exit(0);
    }

    error!(self.log, "Rust proxy: unsupported query type {}", q_string);
    results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
  }
}

// destructor to close connection when Backend goes out of scope
impl Drop for Backend {
  fn drop(&mut self) {
    info!(self.log, "Rust Proxy: Starting destructor for Backend");
    if pelton::close(self.rust_conn) {
      info!(self.log, "Rust Proxy: successfully closed connection");
    } else {
      info!(self.log, "Rust Proxy: failed to close connection");
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
  let flags = pelton::gflags(std::env::args(), USAGE);
  info!(log,
        "Rust proxy: running with args: {:?} {:?} {:?} {:?} {:?}",
        flags.workers,
        flags.consistent,
        flags.db_name,
        flags.hostname,
        flags.db_path);

  let global_open = pelton::initialize(flags.workers, flags.consistent, &flags.db_name, &flags.db_path);
  if !global_open {
    std::process::exit(-1);
  }

  let listener = net::TcpListener::bind(flags.hostname).unwrap();
  info!(log, "Rust Proxy: Listening at: {:?}", listener);
  listener.set_nonblocking(true)
          .expect("Cannot set non-blocking");


  // store client thread handles
  let mut threads = Vec::new();

  // run listener until terminated with SIGTERM
  while !stop.load(Ordering::Relaxed) {
    while let Ok((stream, _addr)) = listener.accept() {
      stream.set_nodelay(true).expect("Cannot disable nagle");
      // clone log so that each client thread has an owned copy
      let log = log.clone();
      threads.push(std::thread::spawn(move || {
                     let bufwriter = io::BufWriter::with_capacity(10000000, stream.try_clone().unwrap());
                     info!(log,
                           "Rust Proxy: Successfully connected to mysql \
                            proxy\nStream and address are: {:?}",
                           stream);
                     let rust_conn = pelton::open();
                     let backend = Backend { rust_conn: rust_conn,
                                             log: log };
                     let _ =
                       MysqlIntermediary::run_on(backend, stream, bufwriter).unwrap();
                   }));
    }
    // wait before checking listener status
    std::thread::sleep(Duration::from_millis(1));
  }

  // join all client threads
  for join_handle in threads {
    join_handle.join().unwrap();
  }
  pelton::shutdown();
}
