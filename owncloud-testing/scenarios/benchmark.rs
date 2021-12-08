#[macro_use]
extern crate clap;
#[macro_use]
extern crate mysql;

extern crate rand;

mod common;
use common::*;
use rand::Rng;


struct User {
    uid: String,
}

struct File<'a> {
    id: usize,
    owned_by: &'a User,
}

struct Group {
    gid: String,
}

enum ShareType<'a> {
    Direct(&'a User),
    Group(&'a Group),
}

struct Share<'a> {
    id: usize,
    share_with: ShareType<'a>,
    file: &'a File<'a>,
}

impl ToValue for Group {
    fn to_value(&self) -> Value {
        self.gid.to_value()
    }
}

impl <'a> Share<'a> {
    fn generate_direct<'b>(users: &'b [User], files: &'b [File<'b>], num: usize) -> Vec<Share<'b>> {
        let ref mut rng = rand::thread_rng();
        files.iter()
            .flat_map(|f| (0..num).zip(std::iter::repeat(f)))
            .enumerate()
            .map(|(id, (_, f))| 
                Share {
                    id,
                    share_with: ShareType::Direct(&users[rng.gen_range(0..users.len())]),
                    file: f
                }
            )
            .collect()
    }
}

impl <'a> InsertDB for Share<'a> {
    fn insert_db(&self, conn: &mut Conn) {
        let (user_target, group_target) = match self.share_with {
            ShareType::Direct(u) => (u.to_value(), Value::NULL),
            ShareType::Group(g) => (Value::NULL, g.to_value()),
        };
        conn.exec_drop(
            "INSERT INTO oc_share 
             VALUES (1, 0, :user_target, :group_target, :owner, :initiator, NULL, 'file', :file, '', '', 24, 0, 0, NULL, '', 1, '', 24, 19);",
            params!(
                user_target,
                group_target,
                "owner" => self.file.owned_by,
                "initiator" => self.file.owned_by,
                "file" => self.file,
            ),
        ).unwrap()
    }
}

impl User {
    fn generate(num: usize) -> Vec<Self> {
        (0..num).map(|i| User { uid: format!("{}", i) }).collect()
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

impl<'a> File<'a> {
    fn generate<'b>(users : &'b [User], num: usize) -> Vec<File<'b>>  {
        users.iter()
            .flat_map(|u| (0..num).zip(std::iter::repeat(u)))
            .enumerate()
            .map(|(id, (_, u))| File {id, owned_by: u})
            .collect()
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


fn main() {

    let matches = 
            clap_app!((crate_name!()) => 
                (version: (crate_version!()))
                (author: (crate_authors!()))
                (@arg num_users: --("num-users") +required +takes_value "Number of users (> 1)")
                (@arg files_per_user: --("files-per-user") +takes_value "Avg files per user (>1)")
                (@arg direct_shares_per_file: --("direct-shares-per-file") +takes_value "Avg direct shares per file")
                (@arg backend: --backend +required +takes_value "Backend type to use")
        ).get_matches();

    let ref mut conn = prepare_backend(match matches.value_of("backend").expect("backend argument is required") {
        "pelton" => Backend::Pelton,
        "mysql" => Backend::MySQL,
        other => panic!("Unexpected value for backend '{}'", other)
    }, true);

    let users = User::generate(value_t_or_exit!(matches, "num_users", usize));
    let files = File::generate(&users, value_t!(matches, "files_per_user", usize).unwrap_or(1));
    let shares = Share::generate_direct(&users, &files, value_t!(matches, "direct_shares_per_file", usize).unwrap_or(1));

    users.insert_db(conn);
    shares.insert_db(conn);
    files.insert_db(conn);


    for user in users.iter() {
        let files : Vec<Row> = conn.query(
            format!("SELECT * FROM file_view WHERE share_target = '{}'", user.uid)
        ).unwrap();
        println!("Files shared with user {}", user.uid);
        for row in files {
            println!("{:?}", row);
        }
    }


    if false {
    std::thread::sleep(std::time::Duration::from_millis(300));
    println!("Dumping database");

    let mut conn = Conn::new(OptsBuilder::new().db_name(Some("pelton")).user(Some("root")).pass(Some("password"))).unwrap();
    for (tab2,) in conn.query("show tables;").unwrap() {
        use std::process::Command;
        use std::io::Write;
        let tab : String = tab2;
        println!("TABLE {}", tab);
        std::io::stdout().write_all(
            &Command::new("mysql").args(["-u", "root", "--password=password", "-B", "pelton", "-e", &format!("SELECT * FROM {};", tab)]).output().unwrap().stdout,
        ).unwrap();
    }
}
}
