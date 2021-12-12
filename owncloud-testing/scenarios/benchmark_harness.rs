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


impl <'a> InsertDB for Share<'a> {
    fn insert_db(&self, conn: &mut Conn, _: &Backend) {
        let (share_type, user_target, group_target) = match self.share_with {
            ShareType::Direct(u) => (0, u.to_value(), Value::NULL),
            ShareType::Group(g) => (1, Value::NULL, g.to_value()),
        };
        conn.exec_drop(
            "INSERT INTO oc_share 
             VALUES (:share_id, :share_type, :user_target, :group_target, :owner, :initiator, NULL, 'file', :file, '', '', 24, 0, 0, NULL, '', 1, '', 24, 19);",
            params!(
                "share_id" => self.id,
                user_target,
                group_target,
                share_type,
                "owner" => self.file.owned_by,
                "initiator" => self.file.owned_by,
                "file" => self.file,
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
        conn.exec_drop("INSERT INTO oc_users VALUES (:uid, :uid, :pw)", params!(
            "uid" => &self.uid,
            "pw" => ""
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
        conn.exec_drop("INSERT INTO oc_files VALUES (?, ?)", (self.id, self.id.to_string())).unwrap()
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
            conn.exec_drop("INSERT INTO oc_groups VALUES (?)", (&self.gid,)).unwrap();
        }
        self.users.iter().for_each(|(i, u)| {
            conn.exec_drop("INSERT INTO oc_group_user VALUES (?, ?, ?)", (i, self, u)).unwrap()
        });
        if backend.is_pelton() {
            conn.exec_drop("INSERT INTO oc_groups VALUES (?)", (&self.gid,)).unwrap();
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
}

fn main() {
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
                (@arg outfile: -o "File for writing output to (defaults to stdout")
                (@arg debug: --debug "Compile and run in debug mode")
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
            .current_dir("../..")
            .spawn()
            .unwrap()
    };
    let mut build = run_bazel_command("build", &vec![]);

    if !build.wait().unwrap().success() {
        panic!("bazel build failed");
    }

    writeln!(f, "Run,Backend,Time").unwrap();

    args.num_users.iter().for_each(|num_users| {

        args.backends.iter().for_each(|backend| {
            let pelton_handle = if backend.is_pelton() {
                std::fs::read_dir("/tmp/pelton/pelton").unwrap().for_each(|p| {
                    let entry = p.unwrap();
                    eprintln!("Deleting {}", entry.path().to_str().unwrap());
                    if entry.file_type().unwrap().is_dir() {
                        std::fs::remove_dir_all(entry.path()).unwrap();
                    } else {
                        std::fs::remove_file(entry.path()).unwrap();
                    }
                });
                Some(
                    CloseProc::new(run_bazel_command("run", &pelton_args))
                )
            } else {
                None
            };

            std::thread::sleep(Duration::from_secs(3));
            let ref mut conn = backend.prepare(true);

            let mut st = GeneratorState::new();
            let users = st.generate_users(*num_users);
            let files = st.generate_files(&users, args.files_per_user);
            let direct_shares = st.generate_direct_shares(&users, &files, args.direct_shares_per_file);
            let groups = st.generate_groups(&users, args.users_per_group);
            let group_shares = st.generate_group_shares(&groups, &files, args.group_shares_per_file);

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

            std::thread::sleep(std::time::Duration::from_millis(300));

            fn select_files_for_user<Q:Queryable>(conn : &mut Q, user: &User) -> Vec<Row> {
                conn.query(
                    format!("SELECT * FROM file_view WHERE share_target = '{}'", user.uid)
                ).unwrap()
            }

            // select_files_for_user(conn, &users[0]);

            // std::thread::sleep(std::time::Duration::from_millis(300));

            let mut rng = rand::thread_rng();
            let ulen = users.len(); 
            let samples = std::iter::repeat_with(|| &users[rng.gen_range(0..ulen)]).take(args.num_samples).collect::<Vec<_>>();
            let now = std::time::Instant::now();
            let _results : Vec<Vec<Row>> = samples.iter().map(|user| select_files_for_user(conn, user)).collect();
            let time = now.elapsed();


            if args.dump_db && backend.is_pelton() {
                pp_pelton_database();
            }
            writeln!(f, "{},{},{}", 0, backend, time.as_millis()).unwrap();
        })
    });
}
