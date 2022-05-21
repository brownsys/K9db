use clap;
use common::{Parameters, ReadRequest, VoteClient, WriteRequest};
use memcached::{
    self,
    proto::{MultiOperation, ProtoType},
};
use mysql_async::prelude::*;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower_service::Service;
use std::collections::BTreeMap;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

enum Req {
    //Populate(Vec<(String, String, String)>),
    //MultiSet(BTreeMap<&[u8], (&'a [u8], u32, u32)>),
    MultiSet(Vec<(String, String)>),
    //Write(Vec<String>),
    Invalidate(Vec<String>),
    Read(Vec<String>),
}

pub struct MemcachedConn{
    //c: mpsc::Sender<(Req, oneshot::Sender<memcached::proto::MemCachedResult<Hashmap<Vec<u8>, (Vec<u8>, u32)>>)>,
    c: mpsc::Sender<(Req, oneshot::Sender<memcached::proto::MemCachedResult<HashMap<Vec<u8>, (Vec<u8>, u32)>>>)>,
    _jh: std::thread::JoinHandle<()>,
    addr: String,
    fast: bool,
}

// The following implementation of hybrid is structured so that if viewd from a distance, this
// clinet behaves similarly to the mysql client(i.e. does all the async stuff by implementing the
// traits that the load generator expects) but internally it also synchronously updates memcached.
// DONE: A new connection on clone for the memcached thingy.
pub struct Conn {
    // Async mysql stuff
    pool: mysql_async::Pool,
    next: Option<mysql_async::Conn>,
    pending: Option<mysql_async::futures::GetConn>,
    mem: MemcachedConn,
}

impl Clone for Conn{
    fn clone(&self) -> Self{
        Conn {
            pool: self.pool.clone(),
            next: None,
            pending: None,
            mem: self.mem.clone(),
       }
    }
}

impl Clone for MemcachedConn{
     fn clone(&self) -> Self {
        // FIXME: this is called once per load _generator_
        // In most cases, we'll have just one generator, and hence only one connection...
        MemcachedConn::new(&self.addr, self.fast).unwrap()
    }
}

impl MemcachedConn{
     fn new(addr: &str, fast: bool) -> memcached::proto::MemCachedResult<Self> {
        eprintln!(
            "OBS OBS OBS: the memcached client is effectively single-threaded -- see src FIXME"
        );

        let connstr = format!("tcp://{}", addr);
        let (reqs, mut rx) =
            mpsc::channel::<(Req, oneshot::Sender<memcached::proto::MemCachedResult<HashMap<Vec<u8>, (Vec<u8>, u32)>>>)>(1);

        let jh = std::thread::spawn(move || {
            let mut c = memcached::Client::connect(&[(&connstr, 1)], ProtoType::Binary).unwrap();

            let mut rt = tokio::runtime::Builder::new()
                .basic_scheduler()
                .enable_all()
                .build()
                .unwrap();
            while let Some((op, ret)) = rt.block_on(rx.recv()) {
                let res = match op {
                    Req::MultiSet(articles) => {
                        let mut m = BTreeMap::new();
                        for &(ref k, ref v) in &articles {
                            m.insert(k.as_bytes(), (v.as_bytes(), 0, 0));
                        }
                        drop(c.set_multi(m));
                        Ok(HashMap::new())
                    }
                    Req::Invalidate(keys) => {
                        let ids: Vec<_> = keys.iter().map(|key| key.as_bytes()).collect();
                        drop(c.delete_multi(&ids[..]));
                        Ok(HashMap::new())
                    }
                    Req::Read(keys) => {
                        let keys: Vec<_> = keys.iter().map(|k| k.as_bytes()).collect();
                        c.get_multi(&keys[..])
                    }
                };
                ret.send(res).unwrap();
            }
        });

        Ok(MemcachedConn {
            c: reqs,
            _jh: jh,
            fast,
            addr: addr.to_string(),
        })
    }
}


impl Conn {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), failure::Error>> {
        if self.next.is_some() {
            return Poll::Ready(Ok(()));
        }

        if self.pending.is_none() {
            self.pending = Some(self.pool.get_conn());
        }

        if let Some(ref mut f) = self.pending {
            match Pin::new(f).poll(cx) {
                Poll::Ready(r) => {
                    self.pending = None;
                    self.next = Some(r?);
                    Poll::Ready(Ok(()))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            unreachable!()
        }
    }
}

impl VoteClient for Conn {
    type Future = impl Future<Output = Result<Self, failure::Error>> + Send;
    fn new(params: Parameters, args: clap::ArgMatches<'_>) -> <Self as VoteClient>::Future {
        let addr = args.value_of("mysql-address").unwrap();
        let addr = format!("mysql://pelton:password@{}", addr);
        let db = args.value_of("database").unwrap().to_string();
        let mem_addr = args.value_of("memcached-address").unwrap().to_string();
        let memcached_conn = MemcachedConn::new(&mem_addr, false);

        async move {
            let opts = mysql_async::Opts::from_url(&addr).unwrap();
            let mut memcached_conn = memcached_conn?;

            if params.prime {
                let mut opts = mysql_async::OptsBuilder::from_opts(opts.clone());
                opts.db_name(None::<&str>);
                //opts.init(vec![
                //    "SET max_heap_table_size = 4294967296;",
                //    "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
                //]);
                let mut conn = mysql_async::Conn::new(opts).await.unwrap();
                let workaround = format!("DROP DATABASE IF EXISTS {}", &db);
                conn = conn.drop_query(&workaround).await.unwrap();
                let workaround = format!("CREATE DATABASE {}", &db);
                conn = conn.drop_query(&workaround).await.unwrap();

                // create tables with indices
                let workaround = format!("USE {}", db);
                conn = conn.drop_query(&workaround).await.unwrap();
                conn = conn
                    .drop_exec(
                        "CREATE TABLE art (id int NOT NULL PRIMARY KEY, title varchar(16) NOT NULL) ENGINE = ROCKSDB",
                        (),
                    )
                    .await
                    .unwrap();
                conn = conn
                    .drop_exec(
                        "CREATE TABLE vt (id int NOT NULL PRIMARY KEY AUTO_INCREMENT, u int NOT NULL, article_id int NOT NULL REFERENCES art(id)) ENGINE = ROCKSDB",
                        (),
                    )
                    .await
                    .unwrap();

                // prepop
                let mut aid = 1;
                let bs = 1000;
                assert_eq!(params.articles % bs, 0);
                for _ in 0..params.articles / bs {
                    let mut sql = String::new();
                    sql.push_str("INSERT INTO art (id, title) VALUES ");
                    for i in 0..bs {
                        if i != 0 {
                            sql.push_str(", ");
                        }
                        sql.push_str("(");
                        sql.push_str(&format!("{}, 'Article #{}')", aid, aid));
                        aid += 1;
                    }
                    conn = conn.drop_query(sql).await.unwrap();
                }
            }

            // Prime memcached as well.
            if params.prime {
            // prepop
            let mut aid = 1;
            let bs = 1000;
            assert_eq!(params.articles % bs, 0);
            for _ in 0..params.articles / bs {
                let articles: Vec<_> = (0..bs)
                    .map(|i| {
                        let article_id = aid + i;
                        (
                            format!("article_{}", article_id),
                            format!("Article #{}, 0", article_id),
                        )
                    })
                    .collect();
                //let mut m = BTreeMap::new();
                //for &(ref k, ref v) in &articles {
                //    m.insert(k.as_bytes(), (v.as_bytes(), 0, 0));
                //}
                let (tx, rx) = oneshot::channel();
                memcached_conn.c
                     .send((Req::MultiSet(articles), tx))
                     .await
                     .unwrap_or_else(|_| panic!("SendError"));
                rx.await.unwrap().unwrap();
                aid += bs;
                }
            }


            // now we connect for real
            let mut opts = mysql_async::OptsBuilder::from_opts(opts);
            opts.db_name(Some(db));
            //opts.init(vec![
            //    "SET max_heap_table_size = 4294967296;",
            //    "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
            //]);
            opts.stmt_cache_size(10000);


            Ok(Conn {
                pool: mysql_async::Pool::new(opts),
                next: None,
                pending: None,
                mem: memcached_conn,
            })
        }
    }
}


impl Service<WriteRequest> for Conn{
    type Response = ();
    type Error = failure::Error;
    type Future = impl Future<Output = Result<(), failure::Error>> + Send;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Conn::poll_ready(self, cx)
    }

    fn call(&mut self, req: WriteRequest) -> Self::Future {
        let mut conn = self.next.take().unwrap();
        let mut mem_channel = self.mem.c.clone();
        async move {
            let ids = req.0.iter().map(|a| a as &_).collect::<Vec<_>>();
            if ids.len() == 0{
                println!("WriteRequest with 0 ids supplied...returning from future");
                return Ok(());
            }
            for article_id in ids.iter() {
                conn = conn.drop_exec(
                    "INSERT INTO vt (u, article_id) VALUES (0, ?)",
                    (article_id,),
                ).await?;
            }

            // Invalid keys in memcached synchronously
            let keys: Vec<_> = req.0.into_iter()
                    .map(|article_id| format!("article_{}", article_id))
                    .collect();
            let (tx, rx) = oneshot::channel();
            mem_channel
                .send((Req::Invalidate(keys), tx))
                .await
                .unwrap_or_else(|_| panic!("SendError"));
            rx.await.unwrap().unwrap();
            // Response
            Ok(())
        }
    }
}

impl Service<ReadRequest> for Conn {
    type Response = ();
    type Error = failure::Error;
    type Future = impl Future<Output = Result<(), failure::Error>> + Send;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Conn::poll_ready(self, cx)
    }

    fn call(&mut self, req: ReadRequest) -> Self::Future {
            let conn = self.next.take().unwrap();
            let mut mem_channel = self.mem.c.clone();
            async move {
            // first read as much as we can from cache
            let len = req.0.len();
            let ids = req.0.iter().map(|a| a as &_).collect::<Vec<_>>();
            let keys: Vec<_> = ids
                .into_iter()
                .map(|article_id| (article_id, format!("article_{}", article_id)))
                .collect();
            let key_strings: Vec<String> = keys.iter().map(|k| k.1.clone()).collect();
            //let key_bytes: Vec<_> = keys.iter().map(|k| k.1.as_bytes()).collect();

            let mut rows = 0;
            let mut misses = Vec::with_capacity(len);
            let (tx, rx) = oneshot::channel();
            mem_channel
                .send((Req::Read(key_strings), tx))
                .await
                .unwrap_or_else(|_| panic!("SendError"));
            let vals: HashMap<Vec<u8>, (Vec<u8>, u32)> = rx.await.unwrap().unwrap();
            //let vals = self.mem.lock().unwrap().get_multi(&key_bytes[..]).unwrap();
            for &(key, ref kstr) in &keys {
                if let Some(_) = vals.get(kstr.as_bytes()) {
                    rows += 1;
                } else {
                    misses.push(key)
                }
            }

        if !misses.is_empty() {
            let ids = misses.iter().map(|a| a as &_).collect::<Vec<_>>();
            let vals = (0..misses.len()).map(|_| "?").collect::<Vec<_>>().join(",");
            //let qstring = format!("SELECT art.* FROM art WHERE art.id IN ({})", vals);
            let qstring = format!("SELECT art.id, art.title, count(*) as votes FROM art JOIN vt ON art.id = vt.article_id GROUP BY vt.article_id HAVING art.id IN ({})", vals);

            let mut m = Vec::new();
            let qresult = conn.prep_exec(qstring, &ids).await.unwrap();
            let result_rows: Vec<mysql_async::Row> = qresult.collect().await.unwrap().1;
            //while(!mysql_async::QueryResult::is_empty(&qresult)){
            //    let (qresult, current_set_rows) = qresult.collect().await.unwrap();
            //    result_rows.extend(current_set_rows);
            //}

            for result_row in result_rows.into_iter(){
              m.push((
                        format!("article_{}", result_row.get::<i64, _>("id").unwrap()),
                        format!(
                            "{}, {}",
                            result_row.get::<String, _>("title").unwrap(),
                            result_row.get::<i64, _>("votes").unwrap()
                        ),
                    ));
                    rows += 1;
            }
            let (tx, rx) = oneshot::channel();
            mem_channel
                .send((Req::MultiSet(m), tx))
                .await
                .unwrap_or_else(|_| panic!("SendError"));
            rx.await.unwrap().unwrap();
           }
            // <= because IN() collapses duplicates
            assert!(rows <= len);
            Ok(())
        }
    }
}
