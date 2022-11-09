use my::prelude::*;
use mysql_async as my;
use std::future::Future;
use trawler::{StoryId, UserId, Vote};

pub async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
    comment: StoryId,
    v: Vote,
    vote_uid: u32,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let c = c.await?;
    let user = acting_as.unwrap();
    let (mut c, comment) = c
        .first_exec::<_, _, my::Row>(
            "SELECT comments.* \
             FROM comments \
             WHERE comments.short_id = ?",
            (format! {"{}", ::std::str::from_utf8(&comment[..]).unwrap()},),
        )
        .await?;

    let comment = comment.unwrap();
    let author = comment.get::<u32, _>("user_id").unwrap();
    let sid = comment.get::<u32, _>("story_id").unwrap();
    let upvotes = comment.get::<i32, _>("upvotes").unwrap();
    let downvotes = comment.get::<i32, _>("downvotes").unwrap();
    let comment = comment.get::<u32, _>("id").unwrap();

    c = c
        .drop_exec(
            "SELECT votes.* \
             FROM votes \
             WHERE votes.user_id = ? \
             AND votes.story_id = ? \
             AND votes.comment_id = ?",
            (user, sid, comment),
        )
        .await?;

    // TODO: do something else if user has already voted
    // TODO: technically need to re-load comment under transaction

    // NOTE: MySQL technically does everything inside this and_then in a transaction,
    // but let's be nice to it
    c = c
        .drop_exec(
            "INSERT INTO votes \
             (id, user_id, story_id, comment_id, vote, reason) \
             VALUES \
             (?, ?, ?, ?, ?, NULL)",
            (
                vote_uid,
                user,
                sid,
                comment,
                match v {
                    Vote::Up => 1,
                    Vote::Down => 0,
                },
            ),
        )
        .await?;

    /*
    c = c
        .drop_exec(
            &format!(
                "UPDATE users \
                 SET users.karma = users.karma {} \
                 WHERE users.id = ?",
                match v {
                    Vote::Up => "+ 1",
                    Vote::Down => "- 1",
                }
            ),
            (author,),
        )
        .await?;
    */

    // approximate Comment::calculate_hotness
    let confidence = (upvotes as f64 / (upvotes as f64 + downvotes as f64)).ceil();
    c = c
        .drop_exec(
            &format!(
                "UPDATE comments \
                 SET \
                 comments.upvotes = comments.upvotes {}, \
                 comments.downvotes = comments.downvotes {}, \
                 comments.confidence = ? \
                 WHERE id = ?",
                match v {
                    Vote::Up => "+ 1",
                    Vote::Down => "+ 0",
                },
                match v {
                    Vote::Up => "+ 0",
                    Vote::Down => "+ 1",
                },
            ),
            (confidence, comment),
        )
        .await?;

    // get all the stuff needed to compute updated hotness
    let (mut c, story) = c
        .first_exec::<_, _, my::Row>(
            "SELECT stories.* \
             FROM stories \
             WHERE stories.id = ?",
            (sid,),
        )
        .await?;
    let story = story.unwrap();
    let score = story.get::<i64, _>("hotness").unwrap();

    c = c
        .drop_exec(
            "SELECT tags.*, taggings.story_id \
             FROM tags \
             INNER JOIN taggings ON tags.id = taggings.tag_id \
             WHERE taggings.story_id = ?",
            (sid,),
        )
        .await?;

    c = c
        .drop_exec(
            "SELECT \
             comments.upvotes, \
             comments.downvotes, comments.story_id \
             FROM comments \
             JOIN stories ON comments.story_id = stories.id \
             WHERE comments.story_id = ? \
             AND comments.user_id != stories.user_id",
            (sid,),
        )
        .await?;

    c = c
        .drop_exec(
            "SELECT stories.id, stories.merged_story_id \
             FROM stories \
             WHERE stories.merged_story_id = ?",
            (sid,),
        )
        .await?;

    // the *actual* algorithm for computing hotness isn't all
    // that interesting to us. it does affect what's on the
    // frontpage, but we're okay with using a more basic
    // upvote/downvote ratio thingy. See Story::calculated_hotness
    // in the lobsters source for details.
    /*
    c = c
        .drop_exec(
            &format!(
                "UPDATE stories SET \
                 stories.upvotes = stories.upvotes {}, \
                 stories.downvotes = stories.downvotes {}, \
                 stories.hotness = ? \
                 WHERE id = ?",
                match v {
                    Vote::Up => "+ 1",
                    Vote::Down => "+ 0",
                },
                match v {
                    Vote::Up => "+ 0",
                    Vote::Down => "+ 1",
                },
            ),
            (
                score
                    - match v {
                        Vote::Up => 1,
                        Vote::Down => -1,
                    },
                sid,
            ),
        )
        .await?;
    */

    Ok((c, false))
}
