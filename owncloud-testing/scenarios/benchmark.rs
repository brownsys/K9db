#[macro_use]
extern crate clap;
#[macro_use]
extern crate mysql;

extern crate rand;

mod common;
use common::*;
use rand::Rng;
use std::collections::HashMap;


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
    fn insert_db(&self, conn: &mut Conn) {
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
    fn insert_db(&self, conn: &mut Conn);
}

impl InsertDB for User {
    fn insert_db(&self, conn: &mut Conn) {
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
    fn insert_db(&self, conn: &mut Conn) {
        conn.exec_drop("INSERT INTO oc_files VALUES (?, ?)", (self.id, self.id.to_string())).unwrap()
    }
}

impl <I:InsertDB> InsertDB for [I] {
    fn insert_db(&self, conn: &mut Conn) {
        self.iter().for_each(|o| o.insert_db(conn))
    }
}

impl <'a, I:InsertDB> InsertDB for &'a I {
    fn insert_db(&self, conn: &mut Conn) {
        (*self).insert_db(conn)
    }
}

impl <'a> InsertDB for Group<'a> {
    fn insert_db(&self, conn: &mut Conn) {
        self.users.iter().for_each(|(i, u)| {
            conn.exec_drop("INSERT INTO oc_group_user VALUES (?, ?, ?)", (i, self, u)).unwrap()
        });
        conn.exec_drop("INSERT INTO oc_groups VALUES (?)", (&self.gid,)).unwrap();
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

fn main() {
    // Add distributions later
    let matches = 
            clap_app!((crate_name!()) => 
                (version: (crate_version!()))
                (author: (crate_authors!()))
                (@arg num_users: --("num-users") +required +takes_value "Number of users (> 1)")
                (@arg files_per_user: --("files-per-user") +takes_value "Avg files per user (>1)")
                (@arg direct_shares_per_file: --("direct-shares-per-file") +takes_value "Avg direct shares per file")
                (@arg group_shares_per_file: --("group-shares-per-file") +takes_value "Avg group shares per file")
                (@arg users_per_group: --("users-per-group") +takes_value "Avg users per group")
                (@arg backend: --backend +required +takes_value "Backend type to use")
                (@arg num_samples: --samples +takes_value "Number of samples to time")
                (@arg dump_db: --("dump-db-after") "Dump the entire mysql database after")
        ).get_matches();

    let ref mut conn = prepare_backend(match matches.value_of("backend").expect("backend argument is required") {
        "pelton" => Backend::Pelton,
        "mysql" => Backend::MySQL,
        other => panic!("Unexpected value for backend '{}'", other)
    }, true);

    let mut st = GeneratorState::new();
    let users = st.generate_users(value_t_or_exit!(matches, "num_users", usize));
    let files = st.generate_files(&users, value_t!(matches, "files_per_user", usize).unwrap_or(1));
    let direct_shares = st.generate_direct_shares(&users, &files, value_t!(matches, "direct_shares_per_file", usize).unwrap_or(1));
    let groups = st.generate_groups(&users, value_t!(matches, "users_per_group", usize).unwrap_or(1));
    let group_shares = st.generate_group_shares(&groups, &files, value_t!(matches, "group_shares_per_file", usize).unwrap_or(1));

    users.insert_db(conn);
    direct_shares.insert_db(conn);
    groups.insert_db(conn);
    group_shares.insert_db(conn);
    files.insert_db(conn);

    std::thread::sleep(std::time::Duration::from_millis(300));

    let mut rng = rand::thread_rng();
    let ulen = users.len(); 
    let samples = std::iter::repeat_with(|| &users[rng.gen_range(0..ulen)]).take(value_t!(matches, "num_samples", usize).unwrap_or(1)).collect::<Vec<_>>();
    let now = std::time::Instant::now();
    let results : Vec<Vec<Row>> = samples.iter().map(|user| {
        conn.query(
            format!("SELECT * FROM file_view WHERE share_target = '{}'", user.uid)
        ).unwrap()
    }).collect();
    let time = now.elapsed();
    println!("Finished lookups in {} milliseconds", time.as_millis());


    if matches.is_present("dump_db") {
        use std::collections::HashSet;
        println!("Dumping database");

        let mut conn = Conn::new(OptsBuilder::new().db_name(Some("pelton")).user(Some("root")).pass(Some("password"))).unwrap();
        let tables : Vec<String> = conn.query("show tables;").unwrap();
        let (user_hashes, mut table_names) : (HashSet<_>, HashSet<_>) = tables.iter().filter_map(|s| s.find('_').map(|i| (&s[..i], &s[(i + 1)..]))).unzip();
        let username_marker_exists = table_names.remove("username_marker");
        for user_hash in user_hashes.iter() {
            let user = if username_marker_exists {
                conn.query_first(format!("SELECT * FROM {}_username_marker;", user_hash)).unwrap().expect("Empty username marker table")
            } else {
                user_hash.to_string()
            };
            println!("μDB for {}", user);
            for tab in table_names.iter() {
                if tab == &"username_marker" { continue; }
                use std::process::Command;
                use std::io::Write;
                println!("TABLE {}", tab);
                std::io::stdout().write_all(
                    &Command::new("mysql").args(["-u", "root", "--password=password", "-B", "pelton", "-e", &format!("SELECT * FROM {}_{};", user_hash, tab)]).output().unwrap().stdout,
                ).unwrap();
            }
            println!("");
        }
    }
}
