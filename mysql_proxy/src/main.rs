use std::io;
use std::net;
use msql_srv::*;
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

        println!("Rust Proxy: calling c-wrapper for pelton::exec\n");
        let exec_response : QueryResponse = exec(&mut self.rust_conn, q_string);
        println!("Rust Proxy: query_response is: {:?}", exec_response);
        let response_type : &CStr = unsafe {CStr::from_ptr(exec_response.response_type)};
        let response_type : &str = response_type.to_str().unwrap();
        println!("Rust Proxy: return type is: {:?}\n", response_type);

        if q_string.contains("SET") {
            return results.completed(0, 0);
        }
        
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
        rw.write_col(42)?; // written to results, writer object to communicate with client. Returning from function just sends to proxy framework (return of function is an emptyio Result)
        // write a value to column "b"
        rw.write_col("b's value")?;
        // tell client no more rows coming. Returns an empty ok to the proxy
        rw.finish()
        // rw.finish(); // client
        // Ok(()) // proxy
    }
}

// custom destructor to close connection once Backend goes out of scope 
impl Drop for Backend {
    fn drop (&mut self) {
        println!("Rust FFI: Calling destructor for Backend");
        println!("Rust Proxy: calling c-wrapper for pelton::close\n");
        let close_response : bool = close(&mut self.rust_conn);
        if (close_response) {
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
            let inter = MysqlIntermediary::run_on_tcp(backend, stream).unwrap();
            // it's not reaching past here because at this point it's already started a new TCP server - it's processing client commands until client disconnects or an error occurs
        }
    });
    
    // wait for listener thread before stopping main. MysqlIntermediary TCP server must terminate before we reach this line
    join_handle.join().unwrap();
    
}
