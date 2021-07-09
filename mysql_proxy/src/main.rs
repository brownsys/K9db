use std::io;
use std::net;
use msql_srv::*;
use std::slice;

include!("wrappers.rs");

// #[derive(Debug)]
struct Backend {
    rust_conn: ConnectionC,
    // exec_result: exec_type,
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
        let exec_response : exec_type = exec(&mut self.rust_conn, q_string);
        
        // determine query type and return appropriate response
        if q_string.contains("CREATE") {
            if unsafe{exec_response.ddl} {
                results.completed(0, 0)
            } else {
                println!("Rust Proxy: Failed to execute ddl");
                results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
            }
        } else if q_string.contains("UPDATE") || q_string.contains("DELETE") || q_string.contains("INSERT") {
            if unsafe{exec_response.update} != -1 {
                unsafe {results.completed(exec_response.update as u64, 0)}
            } else {
                println!("Rust Proxy: Failed to execute update");
                results.error(ErrorKind::ER_INTERNAL_ERROR, &[2])
            }
        } else if q_string.contains("SELECT") {
            // ? destructor for CResult is not getting triggered for some reason
            // ! TODO construct columns based on select response only. Done manually here
            // ! TODO return error on failed select (return -1 to indicate error in C wrapper?)
            let cols = [
                Column {
                    table: "foo".to_string(),
                    column: "a".to_string(),
                    coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                    colflags: ColumnFlags::empty(),
                },
                Column {
                    table: "foo".to_string(),
                    column: "b".to_string(),
                    coltype: ColumnType::MYSQL_TYPE_STRING,
                    colflags: ColumnFlags::empty(),
                },
                ];
                
            // start a resultset response to the client with given columns. ==> replace this with wrapper response
            // Returns a Result<RowWriter<'a,W>> a Struct for sending rows to client. a are the columns, 'a indicates this var has static lifetime equal to lifetime of the program. W is a writer to write data to
            let mut rw = results.start(&cols)?;
            // write a value to the next col of the current row (of this resultset). Here column "a"
            // rw.write_col(42)?; // written to results, writer object to communicate with client. Returning from function just sends to proxy framework (return of function is an emptyio Result)
            
    
            println!("Just before slice conversion");
            // conversion from incomplete array field (flexible array) to rust slice
            // convert outermost arrays (every row) to a slice
            // convert every sub array (every col) to a slice
            // * non-copy approach
            let record_slice = unsafe{(*exec_response.select).records.as_mut_slice((*exec_response.select).num_rows as usize)};
            // empty vec
            // let record_slice2;
            for col in 0..record_slice.len() {
                // slice doesn't copy, converts ptr type to slice. Compile time construct (contiguous)
                record_slice2[col] = std::slice::from_raw_parts_mut(record_slice[col], (*exec_response.select).num_rows as usize);
            }
            // std::vec::with_capacity to make an empty vector. 

            // ! TODO call destructor for CResult manually. select is a mut*
            // drop()

            // *(record_slice[i])
            // * copy approach
            // vec! initializes 
            // let mut record_slice_copy = vec![vec![CResult_RecordData; (*exec_response.select).num_cols as usize]; record_slice.len()];
            // for col in 0..record_slice.len() {
                // record_slice_copy[col] = std::slice::from_raw_parts_mut(record_slice[col], (*exec_response.select).num_rows as usize);
            //     let row = std::slice::from_raw_parts_mut(record_slice[col], (*exec_response.select).num_rows as usize);
            // }
            // ? array of pointers, so can't index twice. Need to convert the ptr to an array as well somehow 
            // let record_val = unsafe {record_slice[0][0].UINT as i32};
            // println!("Just after slice conversion");
            // println!("{:?}", record_val);
            // unsafe{rw.write_col(record_val)?};
            
            // write a value to column "b"
            rw.write_col("b's value")?;
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
    fn drop (&mut self) {
        println!("Rust Proxy: Starting destructor for Backend");
        println!("Rust Proxy: Calling c-wrapper for pelton::close\n");
        let close_response : bool = close(&mut self.rust_conn);
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
        if let Ok((stream, _addr)) = listener.accept() {  // -> .accept returns Result<(TcpStream, SocketAddr)> and blocks thread until a TCP connection is established
            println!("Successfully connected to HotCRP's mysql server\nStream and address are: {:?}\n", stream);
            println!("Rust Proxy: calling c-wrapper for pelton::open\n");
            let rust_conn = open("", "root", "password");
            println!("Rust Proxy: connection status is: {:?}", rust_conn.connected);
            let backend = Backend{rust_conn};
            let _inter = MysqlIntermediary::run_on_tcp(backend, stream).unwrap();
            // it's not reaching past here because at this point it's already started a new TCP server - it's processing client commands until client disconnects or an error occurs
        }
    });
    
    // wait for listener thread before stopping main. MysqlIntermediary TCP server must terminate before we reach this line
    join_handle.join().unwrap();
    
}
