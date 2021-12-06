mod common;
use common::*;

fn main() {
    let prepare_and_insert = true;
    let mut conn = prepare_database(prepare_and_insert);

    let user_id = "claire";
    if prepare_and_insert {
        run_queries_in_str(&mut conn, include_str!("documents_data.sql"));
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


    let files : Vec<Row> = conn.query(
        "SELECT * FROM file_view WHERE share_target = 'wong'"
    ).unwrap();

    for row in files {
        println!("Found share {:?}", row);
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
