extern crate owncloud_scenarios;
use owncloud_scenarios::*;

fn main() {
    let prepare_and_insert = true;
    let ref backend = Backend::Pelton;
    let mut conn = backend.prepare(prepare_and_insert);

    if prepare_and_insert {
        run_queries_in_str(&mut conn, if backend.is_pelton() { include_str!("documents_data.sql") } else { include_str!("documents_data_mysql.sql")});
    }

    // let ref get_files = conn.prep(
    //     "SELECT s.*, f.fileid, f.path, st.id AS storage_string_id
    //      FROM oc_share s
    //      LEFT JOIN oc_filecache f ON s.file_source = f.fileid
    //      LEFT JOIN oc_storages st ON f.storage = st.numeric_id
    //      WHERE ((share_type = '1') AND (share_with IN ?)) OR ((share_type = '0') AND (share_with = ?)) ORDER BY s.id ASC",
    // ).unwrap();
    // let ref get_files = conn.prep(
        // "SELECT s.*, f.fileid, f.path, st.id AS storage_string_id
        //  FROM oc_share s
        //  LEFT JOIN oc_filecache f ON s.file_source = f.fileid
        //  LEFT JOIN oc_storages st ON f.storage = st.numeric_id
        //  WHERE ((share_type = 0) AND (OWNER_share_with = ?)) OR ((share_type = 1) AND (OWNER_share_with_group IN (SELECT OWNING_gid FROM oc_group_user WHERE uid = ?)))
        //  ORDER BY s.id ASC".replace("\n", " "),
        // "SELECT s.*, f.fileid, f.path, st.id AS storage_string_id
        //  FROM oc_share s
        //  LEFT JOIN oc_filecache f ON s.file_source = f.fileid
        //  LEFT JOIN oc_storages st ON f.storage = st.numeric_id
        //  LEFT JOIN oc_group_user ON s.OWNER_share_with_group = oc_group_user.OWNING_gid
        //  WHERE ((s.share_type = 0) AND (s.OWNER_share_with = ?)) 
        //     OR ((s.share_type = 1) AND (oc_group_user.uid = ?))
        //  ORDER BY s.id ASC".replace("\n", " "),
    //     "SELECT * FROM file_view WHERE share_target = ?"
    // ).unwrap();

    // let grps : Vec<String> = conn.exec(
    //     get_groups,
    //     (user_id, )
    // ).unwrap();

    let accessible_files = |conn: &mut Conn, usr| {
        let files : Vec<Row> = conn.query(
            &format!("SELECT * FROM file_view WHERE share_target = '{}'", usr)
        ).unwrap();

        for row in files {
            println!("Found share {:?}", row);
        }
    };

    accessible_files(&mut conn, "wong");

    if backend.is_pelton() {
        println!("Running GDPR get for wong");

        for row in conn.query::<Row,_>("GDPR GET oc_users 'wong'").unwrap() {
            println!("{:?}", row);
        }

        let mut buf = String::new();

        let mut wait_for_enter = |msg| {
            println!("{}", msg);
            std::io::stdin().read_line(&mut buf).unwrap();
            buf.clear();
        };

        conn.query_drop("INSERT INTO oc_files VALUES (3, 'The Goblet of Fire')").unwrap();
        conn.query_drop("INSERT INTO oc_share VALUES (3, 0, 'wong', NULL, 'claire', 'claire', NULL, 'file', 3, '', '', 24, 0, 0, NULL, '', 1, '', 24, 19)").unwrap();

        std::thread::sleep(std::time::Duration::from_secs(1));
        accessible_files(&mut conn, "wong");
        conn.query_drop("GDPR GET oc_users 'wong'").unwrap();

        wait_for_enter("Goblet of Fire (3) Added.");

        conn.query_drop("DELETE FROM oc_share where id = 3").unwrap();
        conn.query_drop("DELETE FROM oc_files where id = 3").unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));

        accessible_files(&mut conn, "wong");
        conn.query_drop("GDPR GET oc_users 'wong'").unwrap();

        wait_for_enter("Goblet of Fire revoked from Wong.");

        conn.query_drop("INSERT INTO oc_group_user VALUES (2, 'sanity', 'wong')").unwrap();
        conn.query_drop("INSERT INTO oc_group_user VALUES (3, 'sanity', 'claire')").unwrap();
        conn.query_drop("INSERT INTO oc_users VALUES ('mindy', 'Mindy', '')").unwrap();
        conn.query_drop("INSERT INTO oc_groups VALUES ('sanity')").unwrap();
        conn.query_drop("INSERT INTO oc_share VALUES (4, 1, NULL, 'sanity', 'mindy', 'mindy', NULL, 'file', 4, '', '', 24, 0, 0, NULL, '', 1, '', 24, 19)").unwrap();
        conn.query_drop("INSERT INTO oc_files VALUES (4, 'The Order of the Phoenix')").unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));
        accessible_files(&mut conn, "wong");

        conn.query_drop("GDPR GET oc_users 'wong'").unwrap();

        wait_for_enter("Order of the phoenix shared with group");

        conn.query_drop("DELETE FROM oc_group_user WHERE id = 2").unwrap();
        std::thread::sleep(std::time::Duration::from_secs(5));
        accessible_files(&mut conn,"wong");
        conn.query_drop("GDPR GET oc_users 'wong'").unwrap();

        wait_for_enter("Removed wong from group.");

        accessible_files(&mut conn, "claire");
        conn.query_drop("GDPR GET oc_users 'claire'").unwrap();

        conn.query_drop("DELETE FROM oc_share WHERE id = 4").unwrap();

        std::thread::sleep(std::time::Duration::from_secs(1));


        conn.query_drop("GDPR GET oc_users 'claire'").unwrap();

        accessible_files(&mut conn, "claire");
        wait_for_enter("Order of the phoenix share revoked");
    }
}
