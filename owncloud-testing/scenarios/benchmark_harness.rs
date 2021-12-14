#[macro_use]
extern crate clap;
#[macro_use]
extern crate mysql;

extern crate rand;

extern crate owncloud_scenarios;

use owncloud_scenarios::*;
use rand::Rng;
use std::collections::HashMap;
use std::process::{Command, Child, Stdio};
use std::io::Write;
use std::time::{Duration, Instant};


struct User {
    uid: String,
}

impl PartialEq for User {
    fn eq(&self, other: &Self) -> bool {
        &self.uid == &other.uid
    }
}

struct File<'a> {
    id: usize,
    owned_by: &'a User,
}

struct Group<'a> {
    gid: String,
    users: Vec<(usize, &'a User)>,
}

enum ShareType<'a> {
    Direct(&'a User),
    Group(&'a Group<'a>),
}

struct Share<'a> {
    id: usize,
    share_with: ShareType<'a>,
    file: &'a File<'a>,
}

impl <'a> ToValue for Group<'a> {
    fn to_value(&self) -> Value {
        self.gid.to_value()
    }
}

fn quoted(s: &str) -> String {
    format!("'{}'", s)
}

impl <'a> InsertDB for Share<'a> {
    fn insert_db(&self, conn: &mut Conn, _: &Backend) {
        let (share_type, user_target, group_target) = match self.share_with {
            ShareType::Direct(u) => (0, quoted(&u.uid), "NULL".to_string()),
            ShareType::Group(g) => (1, "NULL".to_string(), quoted(&g.gid)),
        };
        conn.query_drop(
            format!(
            "INSERT INTO oc_share 
             VALUES ({share_id}, {share_type}, {user_target}, {group_target}, '{owner}', '{initiator}', NULL, 'file', {file}, '', '', 24, 0, 0, NULL, '', 1, '', 24, 19);",
                share_id = self.id,
                user_target = user_target,
                group_target = group_target,
                share_type = share_type,
                owner = self.file.owned_by.uid,
                initiator = self.file.owned_by.uid,
                file = self.file.id,
            ),
        ).unwrap()
    }
}



impl ToValue for User {
    fn to_value(&self) -> Value {
        self.uid.to_value()
    }
}

trait InsertDB {
    fn insert_db(&self, conn: &mut Conn, backend: &Backend);
}

impl InsertDB for User {
    fn insert_db(&self, conn: &mut Conn, _: &Backend) {
        conn.query_drop(&format!("INSERT INTO oc_users VALUES ('{uid}', '{uid}', '{pw}')", 
            uid = &self.uid,
            pw = ""
        )).unwrap();
    }
}



impl <'a> ToValue for File<'a> {
    fn to_value(&self) -> Value {
        self.id.to_value()
    }
}

impl <'a> InsertDB for File<'a> {
    fn insert_db(&self, conn: &mut Conn, _: &Backend) {
        conn.query_drop(&format!("INSERT INTO oc_files VALUES ({}, '{}')", self.id, self.id)).unwrap()
    }
}

impl <I:InsertDB> InsertDB for [I] {
    fn insert_db(&self, conn: &mut Conn, backend: &Backend) {
        self.iter().for_each(|o| o.insert_db(conn, backend))
    }
}

impl <'a, I:InsertDB> InsertDB for &'a I {
    fn insert_db(&self, conn: &mut Conn, backend: &Backend) {
        (*self).insert_db(conn, backend)
    }
}

impl <'a> InsertDB for Group<'a> {
    fn insert_db(&self, conn: &mut Conn, backend: &Backend) {
        if backend.is_mysql() {
            conn.query_drop(&format!("INSERT INTO oc_groups VALUES ('{}')", &self.gid)).unwrap();
        }
        self.users.iter().for_each(|(i, u)| {
            conn.query_drop(&format!("INSERT INTO oc_group_user VALUES ({}, '{}', '{}')", i, self.gid, u.uid)).unwrap()
        });
        if backend.is_pelton() {
            conn.query_drop(&format!("INSERT INTO oc_groups VALUES ('{}')", &self.gid)).unwrap();
        }
    }
}

#[derive(Hash, PartialEq ,Eq)]
enum EntityT {
    Group,
    User,
    Share,
    UserGroupAssoc,
    File,
}

struct GeneratorState(HashMap<EntityT, usize>);

impl GeneratorState {
    fn new() -> Self {
        GeneratorState(HashMap::new())
    }
    fn new_id(&mut self, e: EntityT) -> usize {
        *self.0.entry(e).and_modify(|i| *i += 1).or_insert(0)
    }

    fn generate_groups<'b> (&mut self, users: &'b [User], num: usize) -> Vec<Group<'b>> {
        let ref mut rng = rand::thread_rng();
        let mut raw_groups : Vec<_> = 
                std::iter::repeat_with(||
                    Group { 
                        gid: self.new_id(EntityT::Group).to_string(), 
                        users: vec![] 
                    })
                .take(users.len() / num)
                .collect();
        let grp_len = raw_groups.len();
        users.iter().for_each(|u| 
            raw_groups[rng.gen_range(0..grp_len)].users.push((self.new_id(EntityT::UserGroupAssoc), u))
        );
        raw_groups
    }
    fn generate_files<'b>(&mut self, users : &'b [User], num: usize) -> Vec<File<'b>>  {
        users.iter()
            .flat_map(|u| std::iter::repeat(u).take(num))
            .map(|u| File { id: self.new_id(EntityT::File), owned_by: u})
            .collect()
    }
    fn generate_users(&mut self, num: usize) -> Vec<User> {
        std::iter::repeat_with(|| User { uid: self.new_id(EntityT::User).to_string() }).take(num).collect()
    }
    fn generate_direct_shares<'b>(&mut self, users: &'b [User], files: &'b [File<'b>], num: usize) -> Vec<Share<'b>> {
        let ref mut rng = rand::thread_rng();
        files.iter()
            .flat_map(|f| std::iter::repeat(f).take(num))
            .map(|f|
                Share {
                    id: self.new_id(EntityT::Share),
                    share_with: ShareType::Direct(&users[rng.gen_range(0..users.len())]),
                    file: f
                }
            )
            .collect()
    }
    fn generate_group_shares<'b>(&mut self, groups: &'b [Group], files: &'b[File<'b>], num: usize) -> Vec<Share<'b>> {
        let ref mut rng = rand::thread_rng();
        files.iter()
            .flat_map(|f| std::iter::repeat(f).take(num))
            .map(|f| 
                Share {
                    id: self.new_id(EntityT::Share),
                    share_with: ShareType::Group(&groups[rng.gen_range(0..groups.len())]),
                    file: f
                }
            )
            .collect()
    }
}

struct CloseProc(Child);

impl CloseProc {
    fn new(c: Child) -> Self {
        CloseProc(c)
    }
}

impl std::ops::Deref for CloseProc {
    type Target = Child;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for CloseProc {
    fn drop(&mut self) {
        self.0.kill().unwrap()
    }
}


pub struct Args {
    pub num_users: Vec<usize>,
    pub files_per_user: usize,
    pub direct_shares_per_file: usize,
    pub group_shares_per_file: usize,
    pub users_per_group: usize,
    pub backends: Vec<Backend>,
    pub num_samples: usize,
    pub dump_db: bool,
    pub outfile: Option<String>,
    pub debug: bool,
    pub check_consistency: bool,
}

fn main() {
    let spawn_pelton_yourself = false;
    // Add distributions later
    let matches = 
            clap_app!((crate_name!()) => 
                (version: (crate_version!()))
                (author: (crate_authors!()))
                (@arg num_users: --("num-users") ... +required +takes_value "Number of users (> 1)")
                (@arg files_per_user: --("files-per-user") +takes_value "Avg files per user (>1)")
                (@arg direct_shares_per_file: --("direct-shares-per-file") +takes_value "Avg direct shares per file")
                (@arg group_shares_per_file: --("group-shares-per-file") +takes_value "Avg group shares per file")
                (@arg users_per_group: --("users-per-group") +takes_value "Avg users per group")
                (@arg backend: --backend ... +required +takes_value "Backend type to use")
                (@arg num_samples: --samples +takes_value "Number of samples to time")
                (@arg dump_db: --("dump-db-after") "Dump the entire mysql database after")
                (@arg outfile: -o --out +takes_value "File for writing output to (defaults to stdout")
                (@arg debug: --debug "Compile and run in debug mode")
                (@arg check_consistency: --check "Check consistency between backends")
        ).get_matches();

    let ref args = Args {
        files_per_user: value_t!(matches, "files_per_user", usize).unwrap_or(1),
        direct_shares_per_file: value_t!(matches, "direct_shares_per_file", usize).unwrap_or(1),
        group_shares_per_file: value_t!(matches, "group_shares_per_file", usize).unwrap_or(1),
        users_per_group: value_t!(matches, "users_per_group", usize).unwrap_or(1),
        num_users: values_t_or_exit!(matches, "num_users", usize),
        backends: values_t_or_exit!(matches, "backend", Backend),
        dump_db: matches.is_present("dump_db"),
        num_samples: value_t!(matches, "num_samples", usize).unwrap_or(1),
        outfile: matches.value_of("outfile").map(&str::to_string),
        debug: matches.is_present("debug"),
        check_consistency: matches.is_present("check_consistency"),
    };


    let mut f = if let Some(fname) = &args.outfile {
        Box::new(std::fs::File::create(fname).unwrap()) as Box<dyn Write>
    } else {
        Box::new(std::io::stdout()) as Box<dyn Write>
    };

    let mut bazel_args = vec!["mysql_proxy/src:mysql_proxy"];
    let mut pelton_args = vec![];
    if args.debug {
        pelton_args.push("-alsologtostderr");
    } else {
        bazel_args.push("--config");
        bazel_args.push("opt");
        pelton_args.push("-minloglevel=2");
    }

    let run_bazel_command = |build: &str, run_args: &[&str] | {
        let mut args = vec![build];
        args.extend(bazel_args.iter());
        if !run_args.is_empty() {
            args.push("--");
            args.extend(run_args.iter());
        }
        Command::new("bazel")
            .args(args)
            .current_dir("/home/pelton")
            .spawn()
            .unwrap()
    };
    
    if spawn_pelton_yourself {
        let mut build = run_bazel_command("build", &vec![]);

        if !build.wait().unwrap().success() {
            panic!("bazel build failed");
        }
    }
    let num_users = &args.num_users[0];

    let mut st = GeneratorState::new();
    let users = st.generate_users(*num_users);
    let files = st.generate_files(&users, args.files_per_user);
    let direct_shares = st.generate_direct_shares(&users, &files, args.direct_shares_per_file);
    let groups = st.generate_groups(&users, args.users_per_group);
    let group_shares = st.generate_group_shares(&groups, &files, args.group_shares_per_file);

    let mut rng = rand::thread_rng();
    let ulen = users.len(); 
    let mut samples : Vec<&User> = Vec::with_capacity(args.num_samples); 
    if users.len() < args.num_samples {
        panic!("Too few users, need at least as many as samples");
    }
    while samples.len() != args.num_samples {
        let u = &users[rng.gen_range(0..ulen)];
        if !samples.contains(&u) {
            samples.push(u);
        }
    }

    writeln!(f, "Run,Backend,Time(ms)").unwrap();


    let mut results : Vec<Vec<Vec<Row>>> = args.backends.iter().map(|backend| {
        let pelton_handle = if spawn_pelton_yourself && backend.is_pelton() {
            let rm_res = std::fs::remove_dir_all("/var/pelton/rocksdb/pelton");
            if let Err(e) = &rm_res {
                if e.kind() != std::io::ErrorKind::NotFound {
                    panic!("{}", e);
                }
            }
            Some(
                CloseProc::new(run_bazel_command("run", &pelton_args))
            )
        } else {
            None
        };

        std::thread::sleep(Duration::from_secs(3));
        let ref mut conn = backend.prepare(true);

        let i_ins = Instant::now();
        users.insert_db(conn, backend);
        if backend.is_pelton() {
            direct_shares.insert_db(conn, backend);
            groups.insert_db(conn, backend);
            group_shares.insert_db(conn, backend);
            files.insert_db(conn, backend);
        } else {
            files.insert_db(conn, backend);
            direct_shares.insert_db(conn, backend);
            groups.insert_db(conn, backend);
            group_shares.insert_db(conn, backend);
        }
        let t_ins = i_ins.elapsed();
        //conn.query_drop("SET echo");
        eprintln!("Inserts done in {}ms", t_ins.as_millis());

        //std::io::stdin().read_line(&mut String::new());

        fn select_files_for_user<Q:Queryable>(conn : &mut Q, user: &User) -> Vec<Row> {
            conn.query(
                format!("SELECT * FROM file_view WHERE share_target = '{}'", user.uid)
            ).unwrap()
        }

        // select_files_for_user(conn, &users[0]);

        // std::thread::sleep(std::time::Duration::from_millis(300));
        let mut sstring = samples.iter().map(|s| format!("'{}'", s.uid)).collect::<Vec<_>>();
        //sstring.extend(std::iter::repeat("'-1'".to_string()).take(args.num_samples));
        let query = format!("SELECT * FROM file_view WHERE share_target IN ({})", sstring.join(","));
        //eprintln!("QUERY: {}", query);
        let now = std::time::Instant::now();
        let vs : Vec<Row> =
        
                conn.query(query).unwrap();
        
        let time = now.elapsed();
        let results = vec![];

        eprintln!("Returned {} rows", vs.len());

        if args.dump_db && backend.is_pelton() {
            pp_pelton_database();
        }
        writeln!(f, "{},{},{}", 0, backend, time.as_millis()).unwrap();

        results
    }).collect::<Vec<_>>();

    if results.len() == 2 && args.check_consistency {
        let res1 = results.pop().unwrap().into_iter();
        let res2 = results.pop().unwrap().into_iter();
        for (mut r1, mut r2) in res1.zip(res2) {
            for (f1, f2) in r1.into_iter().zip(r2.into_iter()) {
                if f1 != f2 {
                    eprintln!("{:?} != {:?}", f1.unwrap(), f2.unwrap());
                }
            }
        }
    }
}
