use chrono;
use my::prelude::*;
use mysql_async as my;
use std::future::Future;
use trawler::{StoryId, UserId};

pub async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    id: StoryId,
    title: String,
    priming: bool,
    story_uid: u32,
    taggings_uid: u32,
    votes_uid: u32,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let c = c.await?;
    let user = acting_as.unwrap();

    // check that tags are active
    let (mut c, tag) = c
        .first::<_, my::Row>(
            "SELECT tags.* FROM tags \
             WHERE tags.inactive = 0 AND tags.tag IN ('test')",
        )
        .await?;
    let tag = tag.unwrap();
    let tag = tag.get::<u32, _>("id");
    let tag_id = tag.unwrap();

    if !priming {
        // check that story id isn't already assigned
        c = c
            .drop_exec(
                "SELECT 1 AS `one`, stories.short_id FROM stories \
                 WHERE stories.short_id = ?",
                (format! {"{}", ::std::str::from_utf8(&id[..]).unwrap()},),
            )
            .await?;
    }

    // TODO: check for similar stories if there's a url
    // SELECT stories.*
    // FROM stories
    // WHERE stories.url IN (
    //  'https://google.com/test',
    //  'http://google.com/test',
    //  'https://google.com/test/',
    //  'http://google.com/test/',
    //  ... etc
    // )
    // AND (is_expired = 0 OR is_moderated = 1)

    // TODO
    // real impl queries tags and users again here..?

    // TODO: real impl checks *new* short_id and duplicate urls *again*
    // TODO: sometimes submit url

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    // let mut rng = rand::thread_rng();
    let story = story_uid;
    let q = c
        .prep_exec(
            "INSERT INTO stories \
             (id, created_at, user_id, title, \
             description, short_id, upvotes, hotness, \
             markeddown_description,\
             url, is_expired, downvotes, is_moderated, comments_count,\
             story_cache, merged_story_id, unavailable_at, twitter_id, user_is_author) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL)",
            (
                story,
                format!("{}", chrono::Local::now().naive_local()),
                user,
                format! {"{}", title},
                "to infinity", // lorem ipsum?
                format! {"{}", ::std::str::from_utf8(&id[..]).unwrap()},
                1,
                19217,
                "<p>to infinity</p>",
            ),
        )
        .await?;
    // let _story_unused = q.last_insert_id().unwrap();
    let mut c = q.drop_result().await?;

    c = c
        .drop_exec(
            "INSERT INTO taggings (id, story_id, tag_id) \
             VALUES (?, ?, ?)",
            (taggings_uid, story, tag_id),
        )
        .await?;

    let key = format!("user:{}:stories_submitted", user);
    c = c
        .drop_exec(
            "REPLACE INTO keystores (keyX, valueX) \
             VALUES (?, ?)",
            (key, 1),
        )
        .await?;

    if !priming {
        let key = format!("user:{}:stories_submitted", user);
        c = c
            .drop_exec(
                "SELECT keystores.* \
                 FROM keystores \
                 WHERE keystores.keyX = ?",
                (format!("{}", key),),
            )
            .await?;

        c = c
            .drop_exec(
                "SELECT votes.* FROM votes \
                 WHERE votes.OWNER_user_id = ? \
                 AND votes.story_id = ? \
                 AND votes.comment_id IS NULL",
                (user, story),
            )
            .await?;
    }

    c = c
        .drop_exec(
            "INSERT INTO votes \
             (id, OWNER_user_id, story_id, vote, comment_id, reason) \
             VALUES (?, ?, ?, ?, NULL, NULL)",
            (votes_uid, user, story, 1),
        )
        .await?;

    if !priming {
        c = c
            .drop_exec(
                "SELECT \
                 comments.upvotes, \
                 comments.downvotes, comments.story_id \
                 FROM comments \
                 JOIN stories ON comments.story_id = stories.id \
                 WHERE comments.story_id = ? \
                 AND comments.user_id != stories.user_id",
                (story,),
            )
            .await?;

        // why oh why is story hotness *updated* here?!
        c = c
            .drop_exec(
                "UPDATE stories \
                 SET hotness = ? \
                 WHERE stories.id = ?",
                (19217, story),
            )
            .await?;
    }

    Ok((c, false))
}
