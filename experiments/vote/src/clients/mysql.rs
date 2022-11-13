use clap;
use common::{Parameters, ReadRequest, VoteClient, WriteRequest};
use mysql_async::prelude::*;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower_service::Service;

pub struct Conn {
    pool: mysql_async::Pool,
    next: Option<mysql_async::Conn>,
    pending: Option<mysql_async::futures::GetConn>,
}

impl Clone for Conn {
    fn clone(&self) -> Self {
        Conn {
            pool: self.pool.clone(),
            next: None,
            pending: None,
        }
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
        let addr = args.value_of("address").unwrap();
        let addr = format!("mysql://pelton:password@{}", addr);
        let db = args.value_of("database").unwrap().to_string();

        async move {
            let opts = mysql_async::Opts::from_url(&addr).unwrap();

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
            })
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
        async move {
            let len = req.0.len();
            let ids = req.0.iter().map(|a| a as &_).collect::<Vec<_>>();
            let vals = (0..len).map(|_| "?").collect::<Vec<_>>().join(",");
            let qstring = format!("SELECT art.id, art.title, count(*) as votes FROM art JOIN vt ON art.id = vt.article_id GROUP BY vt.article_id HAVING art.id IN ({})", vals);

            let qresult = conn.prep_exec(qstring, &ids).await.unwrap();
            let (_, rows) = qresult
                .reduce_and_drop(0, |rows, _| rows + 1)
                .await
                .unwrap();

            // <= because IN() collapses duplicates
            assert!(rows <= ids.len());

            Ok(())
        }
    }
}

impl Service<WriteRequest> for Conn {
    type Response = ();
    type Error = failure::Error;
    type Future = impl Future<Output = Result<(), failure::Error>> + Send;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Conn::poll_ready(self, cx)
    }

    fn call(&mut self, req: WriteRequest) -> Self::Future {
        let mut conn = self.next.take().unwrap();
        async move {
            //let len = req.0.len();
            let ids = req.0.iter().map(|a| a as &_).collect::<Vec<_>>();
            if ids.len() == 0 {
                println!("WriteRequest with 0 ids supplied...returning from future.")
            }
            for article_id in ids.iter() {
                conn = conn
                    .drop_exec(
                        "INSERT INTO vt (u, article_id) VALUES (0, ?)",
                        (article_id,),
                    )
                    .await?;
            }
            //let vals = (0..len).map(|_| "(0, ?)").collect::<Vec<_>>().join(", ");
            //let vote_qstring = format!("INSERT INTO vt (u, article_id) VALUES {}", vals);
            //conn.drop_exec(vote_qstring, &ids).await.unwrap();
            Ok(())
        }
    }
}
