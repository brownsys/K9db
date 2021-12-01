mod common;
use common::*;

fn main() {
    let mut conn = prepare_database();

    let share_target = "janine";
    let file_owner = "claire";
    let shared_by = file_owner;
    let file_name = "/book/The Sorcerers Stone.ebook";
    let folder_url = "https://shared.space/apps/files/?dir=/";

    let ref insert_user = conn.prep(
        "INSERT INTO oc_users (uid, displayname, password) VALUES (?, ?, ?)",
    ).unwrap();

    let ref do_share = conn.prep(
        "INSERT INTO `oc_share` (`share_type`, `share_with`, `accepted`, `item_type`, `item_source`, `file_source`, `permissions`, `attributes`, `uid_initiator`, `uid_owner`, `file_target`, `stime`) VALUES ('0', ?, 0, 'file', '24', '24', '19', NULL, ?, ?, ?, ?)",
    ).unwrap();

    let ref get_share = conn.prep(
        "SELECT * FROM `oc_share` WHERE `id` = ?",
    ).unwrap();

    let ref record_share_activity = conn.prep(
        "INSERT INTO `oc_activity` (`app`, `subject`, `subjectparams`, `message`, `messageparams`, `file`, `link`, `user`, `affecteduser`, `timestamp`, `priority`, `type`, `object_type`, `object_id`) VALUES ('files_sharing', 'shared_user_self', ?, '', '[]', ?, ?, ?, ?, ?, '30', 'shared', 'files', ?)",
    ).unwrap();

    let record_share_activity_mq = conn.prep(
        "INSERT INTO `oc_activity_mq` (`amq_appid`, `amq_subject`, `amq_subjectparams`, `amq_affecteduser`, `amq_timestamp`, `amq_type`, `amq_latest_send`) VALUES ('files_sharing', 'shared_with_by', ?, ?, ?, 'shared', ?)"
    ).unwrap();

    conn.exec_drop(insert_user, (share_target, share_target, "abc")).unwrap();
    conn.exec_drop(insert_user, (file_owner, file_owner, "abc")).unwrap();

    let time = chrono::Utc::now().timestamp();

    {
        let mut t = conn.start_transaction(TxOpts::default()).unwrap();
        let share_id : u64 = t.exec_first(
            do_share,
            (share_target, shared_by, file_owner, file_name, time),
        ).unwrap().unwrap();

        let _share : Row = t.exec_first(
            get_share,
            (share_id,)
        ).unwrap().unwrap();
        t.commit();
    }

    let obj_id = 24;

    conn.exec_drop(
        record_share_activity,
        ("shared_user_self",
         format!("[{{\"{}\":\"\\{}\"}},{}]", obj_id, file_name, share_target),
         file_name,
         folder_url,
         file_owner,
         file_owner,
         time,
         obj_id,
        ),
    ).unwrap();

    conn.exec_drop(
        record_share_activity,
        ("shared_with_by",
         format!("[{{\"{}\":\"\\{}\"}},{}]", obj_id, file_name, file_owner),
         file_name,
         folder_url,
         file_owner,
         share_target,
         time,
         obj_id,
        ),
    ).unwrap();

    conn.exec_drop(
        record_share_activity_mq,
        (format!("[{{\"{}\":\"\\{}\"}},{}]", obj_id, file_name, file_owner),
         share_target,
         time,
         time,
        )
    );

}
