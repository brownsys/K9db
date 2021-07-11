use msql_srv::*;
use std::io;
use std::net;
use std::ptr;
use std::slice;

include!("wrappers.rs");

#[derive(Debug)]
struct Backend {
    rust_conn: ConnectionC,
}

impl<W: io::Write> MysqlShim<W> for Backend {
    type Error = io::Error;

    // called when client issues request to prepare query for later execution
    fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        println!("on_prepare");
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
        println!("on_execute");
        results.completed(0, 0)
    }

    // called when client wants to deallocate resources associated with a prev prepared statement
    fn on_close(&mut self, _: u32) {
        println!("on_close");
    }

    // called when client switches databases
    fn on_init(&mut self, _: &str, writer: InitWriter<W>) -> io::Result<()> {
        // first statement in sql would be `use db` => acknowledgement
        println!("on_init");
        // Tell client that database context has been changed
        writer.ok()
        // not returned to client, but to a component inside the proxy mysql_srv that does erorr checking
        // only way to send to client is via the writer InitWriter object.
        // Ok(())
    }

    // called when client issues query for immediate execution. Results returned via QueryResultWriter
    fn on_query(&mut self, q_string: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        println!("Rust proxy: starting on_query");
        println!("Rust Proxy: query received from HotCRP is: {:?}", q_string);

        if q_string.contains("SET") || q_string.contains("DROP") || q_string.contains("ALTER") {
            println!("Rust Proxy: Unsupported query type");
            return results.error(ErrorKind::ER_INTERNAL_ERROR, &[2]);
        }

        println!("Rust Proxy: calling rust wrapper that calls c-wrapper for pelton::exec\n");
        let exec_response: exec_type = exec(&mut self.rust_conn, q_string);

        // determine query type and return appropriate response
        if q_string.contains("CREATE") {
            if unsafe { exec_response.ddl } {
                results.completed(0, 0)
            } else {
                println!("Rust Proxy: Failed to execute ddl");
                results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
            }
        } else if q_string.contains("UPDATE")
            || q_string.contains("DELETE")
            || q_string.contains("INSERT")
        {
            if unsafe { exec_response.update } != -1 {
                unsafe { results.completed(exec_response.update as u64, 0) }
            } else {
                println!("Rust Proxy: Failed to execute update");
                results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
            }
        } else if q_string.contains("SELECT") {
            println!("Rust Proxy: Populating mysql_srv response");
            // ! TODO return error on failed select (return NULL to indicate error in C wrapper?) Or how mimic rust's Result type?

            // ? am I freeing the char* strings correctly in destroy_select in the C-Wrapper?
            // add destructor for strings passed to rust? Or automatically destructed when take ownership via .to_owned?
            // => might be cloned so still need to free the malloced memory. 

            let num_cols = unsafe { (*exec_response.select).num_cols as usize };
            let num_rows = unsafe { (*exec_response.select).num_rows as usize };
            let col_names = unsafe { (*exec_response.select).col_names };
            let col_types = unsafe { (*exec_response.select).col_types };

            let mut cols = Vec::with_capacity(num_cols);

            
            println!("Rust Proxy: creating columns");
            for c in 0..num_cols {
                // convert table_name to rust string
                let table_name = unsafe{(*exec_response.select).table_name};
                let table_name_string : String = unsafe{CStr::from_ptr(table_name).to_str().unwrap().to_owned()};
                // convert col_name at index c to rust string
                let col_name_string: String =
                    unsafe { CStr::from_ptr(col_names[c]).to_str().unwrap().to_owned() };
                
                // convert C column type enum to MYSQL type
                let col_type_c = col_types[c];
                let col_type = match col_type_c {
                    ColumnDefinitionTypeEnum_UINT => ColumnType::MYSQL_TYPE_LONGLONG,
                    ColumnDefinitionTypeEnum_INT => ColumnType::MYSQL_TYPE_LONGLONG,
                    ColumnDefinitionTypeEnum_TEXT => ColumnType::MYSQL_TYPE_STRING,
                    ColumnDefinitionTypeEnum_DATETIME => ColumnType::MYSQL_TYPE_STRING,
                    _ => ColumnType::MYSQL_TYPE_NULL,
                };
                cols.push(Column {
                    table: table_name_string,
                    // table: "my_table".to_string(),
                    column: col_name_string,
                    coltype: col_type,
                    colflags: ColumnFlags::empty(),
                });
            }

            // start a resultset response to the client with given columns. ==> replace this with wrapper response
            // Returns a Result<RowWriter<'a,W>> a Struct for sending rows to client. a are the columns, 'a indicates this var has static lifetime equal to lifetime of the program. W is a writer to write data to
            let mut rw = results.start(&cols)?;

            println!("Rust Proxy: writing select response using RowWriter");
            // conversion from incomplete array field (flexible array) to rust slice
            let mut rows_vector: Vec<&[*mut CResult_RecordData]> = Vec::with_capacity(num_rows);
            // converting outer array of rows to slice
            let rows_slice = unsafe { (*exec_response.select).records.as_slice(num_rows) };

            for r in 0..num_rows {
                // converting inner array of columns (for this row) to slice
                let col_slice = unsafe { std::slice::from_raw_parts(rows_slice[r], num_cols) };
                for c in 0..num_cols {
                    match col_types[c] {
                        // write a value to the next col of the current row (of this resultset)
                        ColumnDefinitionTypeEnum_UINT => unsafe {
                            rw.write_col(col_slice[c].UINT)?
                        },
                        ColumnDefinitionTypeEnum_INT => unsafe { rw.write_col(col_slice[c].INT)? },
                        ColumnDefinitionTypeEnum_TEXT => unsafe {
                            rw.write_col(
                                CStr::from_ptr(col_slice[c].TEXT)
                                    .to_str()
                                    .unwrap()
                                    .to_owned(),
                            )?
                        },
                        ColumnDefinitionTypeEnum_DATETIME => unsafe {
                            rw.write_col(
                                CStr::from_ptr(col_slice[c].DATETIME)
                                    .to_str()
                                    .unwrap()
                                    .to_owned(),
                            )?
                        },
                        _ => println!("Rust Proxy: Invalid column type"),
                    }
                }
                rw.end_row()?;
            }
            // calling destructor for CResult
            unsafe { std::ptr::drop_in_place(exec_response.select) };
            // tell client no more rows coming. Returns an empty ok to the proxy
            rw.finish()
            // rw.finish(); // client
            // Ok(()) // proxy
        } else {
            println!("Rust proxy: unsupported query type");
            results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
        }
    }
}

// custom destructor to close connection once Backend goes out of scope
impl Drop for Backend {
    fn drop(&mut self) {
        println!("Rust Proxy: Starting destructor for Backend");
        println!("Rust Proxy: Calling c-wrapper for pelton::close\n");
        let close_response: bool = close(&mut self.rust_conn);
        if close_response {
            println!("Rust Proxy: successfully closed connection");
        } else {
            println!("Rust Proxy: failed to close connection");
        }
    }
}

fn main() {
    let listener = net::TcpListener::bind("127.0.0.1:10001").unwrap();
    // let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
    println!("Listening at: {:?}", listener);

    // thread::spawn creates a new thread with the first function it should run
    // move keyword means new thread takes ownership of vars it uses (in this case listener)
    let join_handle = std::thread::spawn(move || {
        // Ok wraps around another value --> Result type exists in one of two states: Ok or Err, both of which wrap around another type, indicating if it represents an success or an error
        // the next line reads: "if `let` destructures `listener.accept()` into `Ok((stream, addr))`then evaluate the block {}"
        // i.e. if connection established, call mysql_intermediary's run_on_tcp. (_ in front of a variable name indicates to compiler that this is unused)
        if let Ok((stream, _addr)) = listener.accept() {
            // -> .accept returns Result<(TcpStream, SocketAddr)> and blocks thread until a TCP connection is established
            println!(
                "Successfully connected to HotCRP's mysql server\nStream and address are: {:?}\n",
                stream
            );
            println!("Rust Proxy: calling c-wrapper for pelton::open\n");
            let rust_conn = open("", "root", "password");
            println!(
                "Rust Proxy: connection status is: {:?}",
                rust_conn.connected
            );
            let backend = Backend { rust_conn };
            let _inter = MysqlIntermediary::run_on_tcp(backend, stream).unwrap();
            // it's not reaching past here because at this point it's already started a new TCP server - it's processing client commands until client disconnects or an error occurs
        }
    });

    // wait for listener thread before stopping main. MysqlIntermediary TCP server must terminate before we reach this line
    join_handle.join().unwrap();
}
