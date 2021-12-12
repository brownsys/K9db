extern crate owncloud_scenarios;
use owncloud_scenarios::*;

fn main() {
    let prepare_and_insert = true;
    let ref backend = Backend::MySQL;
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


    let files : Vec<Row> = conn.query(
        "SELECT * FROM file_view WHERE share_target = 'wong'"
    ).unwrap();

    for row in files {
        println!("Found share {:?}", row);
    }

    if backend.is_pelton() {
        println!("Running GDPR get for wong");

        for row in conn.query::<Row,_>("GDPR GET oc_users 'wong'").unwrap() {
            println!("{:?}", row);
        }

        pp_pelton_database()
    }
}
