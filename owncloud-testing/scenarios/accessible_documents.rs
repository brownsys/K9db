mod common;
use common::*;

fn main() {
    let mut conn = prepare_database();

    let get_groups = conn.prep(
        "SELECT `gid` FROM `oc_group_user` WHERE `uid` = ?",
    ).unwrap();
    let get_files = conn.prep(
        "SELECT s.*, f.`fileid`, f.`path`, st.`id` AS `storage_string_id`
         FROM `oc_share` s
         LEFT JOIN `oc_filecache` f ON s.`file_source` = f.`fileid`
         LEFT JOIN `oc_storages` st ON f.`storage` = st.`numeric_id`
         WHERE ((`share_type` = '1') AND (`share_with` IN ?)) OR ((`share_type` = '0') AND (`share_with` = ?)) ORDER BY s.`id` ASC",
    ).unwrap();

    let user_id = "admin";

    let grps : Vec<String> = conn.exec(
        get_groups,
        (user_id, )
    ).unwrap();

    let grps_as_val : Value = unimplemented!("This should be this vector {:?}", grps);

    let files : Vec<Row> = conn.exec(
        get_files,
        (grps_as_val, user_id)
    ).unwrap();
}
