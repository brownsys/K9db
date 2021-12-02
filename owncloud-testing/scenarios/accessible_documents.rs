mod common;
use common::*;

fn main() {
    let mut conn = prepare_database();

    let user_id = "claire";

    run_queries_in_str(&mut conn, include_str!("documents_data.sql"));
    // let ref get_groups = conn.prep(
    //     "SELECT gid FROM oc_group_user WHERE uid = ?",
    // ).unwrap();
    // let ref get_files = conn.prep(
    //     "SELECT s.*, f.fileid, f.path, st.id AS storage_string_id
    //      FROM oc_share s
    //      LEFT JOIN oc_filecache f ON s.file_source = f.fileid
    //      LEFT JOIN oc_storages st ON f.storage = st.numeric_id
    //      WHERE ((share_type = '1') AND (share_with IN ?)) OR ((share_type = '0') AND (share_with = ?)) ORDER BY s.id ASC",
    // ).unwrap();
    let ref get_files = conn.prep(
        "SELECT s.*, f.fileid, f.path, st.id AS storage_string_id
         FROM oc_share s
         LEFT JOIN oc_filecache f ON s.file_source = f.fileid
         LEFT JOIN oc_storages st ON f.storage = st.numeric_id
         WHERE ((share_type = '0') AND (share_with = ?)) ORDER BY s.id ASC",
    ).unwrap();

    // let grps : Vec<String> = conn.exec(
    //     get_groups,
    //     (user_id, )
    // ).unwrap();

    // FIXME This should be the entire vector, but value cannot hold a vector. 
    //let grps_as_val : Value = (&grps[0]).into();

    let _files : Vec<Row> = conn.exec(
        get_files,
        (user_id,)
    ).unwrap();
}
