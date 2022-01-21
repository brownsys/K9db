use my::prelude::*;
use mysql_async as my;
use std::future::Future;
use trawler::UserId;

pub async fn handle<F>(
    c: F,
    _acting_as: Option<UserId>,
    uid: UserId,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let c = c.await?;
    let (mut c, user) = c
        .first_exec::<_, _, my::Row>(
            "SELECT users.* FROM users \
             WHERE users.PII_username = ?",
            (format!("user{}", uid),),
        )
        .await?;
    let uid = user
        .expect(&format!("user {} should exist", uid))
        .get::<u32, _>("id")
        .unwrap();

    // most popular tag
    c = c
        .drop_exec(
            "SELECT tags.id, stories.user_id, count(*) AS `count` FROM tags \
             INNER JOIN taggings ON tags.id = taggings.tag_id \
             INNER JOIN stories ON taggings.story_id = stories.id \
             WHERE tags.inactive = 0 \
             AND stories.user_id = ? \
             GROUP BY tags.id, stories.user_id \
             ORDER BY `count` DESC LIMIT 1",
            (uid,),
        )
        .await?;

    c = c
        .drop_exec(
            "SELECT keystores.* \
             FROM keystores \
             WHERE keystores.keyX = ?",
            (format!("user:{}:stories_submitted", uid),),
        )
        .await?;

    c = c
        .drop_exec(
            "SELECT keystores.* \
             FROM keystores \
             WHERE keystores.keyX = ?",
            (format!("user:{}:comments_posted", uid),),
        )
        .await?;

    c = c
        .drop_exec(
            "SELECT 1 AS `one`, hats.OWNER_user_id FROM hats \
             WHERE hats.OWNER_user_id = ? LIMIT 1",
            (uid,),
        )
        .await?;

    Ok((c, true))
}
