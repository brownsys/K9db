use my::prelude::*;
use mysql_async as my;
use std::collections::HashSet;
use std::future::Future;
use std::iter;
use trawler::UserId;

pub async fn handle<F>(
    c: F,
    acting_as: Option<UserId>,
) -> Result<(my::Conn, bool), my::error::Error>
where
    F: 'static + Future<Output = Result<my::Conn, my::error::Error>> + Send,
{
    let c = c.await?;
    let comments = c
        .prep_exec(
            "SELECT comments.* \
         FROM comments \
         WHERE comments.is_deleted = 0 \
         AND comments.is_moderated = 0 \
         ORDER BY id DESC \
         LIMIT 40",
            (),
        )
        .await?;

    let (mut c, (comments, users, stories)) = comments
        .reduce_and_drop(
            (Vec::new(), HashSet::new(), HashSet::new()),
            |(mut comments, mut users, mut stories), comment| {
                comments.push(comment.get::<u32, _>("id").unwrap());
                users.insert(comment.get::<u32, _>("user_id").unwrap());
                stories.insert(comment.get::<u32, _>("story_id").unwrap());
                (comments, users, stories)
            },
        )
        .await?;

    if let Some(uid) = acting_as {
        let params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let args: Vec<&UserId> = iter::once(&uid as &UserId)
            .chain(stories.iter().map(|c| c as &UserId))
            .collect();
        c = c
            .drop_exec(
                &format!(
                    "SELECT 1, user_id, story_id FROM hidden_stories \
                     WHERE hidden_stories.user_id = ? \
                     AND hidden_stories.story_id IN ({})",
                    params
                ),
                args,
            )
            .await?;
    }

    let users = users
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");
    c = c
        .drop_query(&format!(
            "SELECT users.* FROM users \
             WHERE users.id IN ({})",
            users
        ))
        .await?;

    // let stories = stories
    //     .into_iter()
    //     .map(|id| format!("{}", id))
    //     .collect::<Vec<_>>()
    //     .join(",");

    let story_params = stories.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let stories: Vec<&u32> = stories.iter().map(|s| s as &_).collect();

    let stories = c
        .prep_exec(
            &format!(
                "SELECT stories.* FROM stories \
             WHERE stories.id IN ({})",
                story_params
            ),
            stories,
        )
        .await?;

    let (mut c, authors) = stories
        .reduce_and_drop(HashSet::new(), |mut authors, story| {
            authors.insert(story.get::<u32, _>("user_id").unwrap());
            authors
        })
        .await?;

    if let Some(uid) = acting_as {
        let params = comments.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let comments: Vec<&UserId> = iter::once(&uid as &UserId)
            .chain(comments.iter().map(|c| c as &UserId))
            .collect();
        c = c
            .drop_exec(
                &format!(
                    "SELECT votes.* FROM votes \
                     WHERE votes.OWNER_user_id = ? \
                     AND votes.comment_id IN ({})",
                    params
                ),
                comments,
            )
            .await?;
    }

    // NOTE: the real website issues all of these one by one...
    let authors = authors
        .into_iter()
        .map(|id| format!("{}", id))
        .collect::<Vec<_>>()
        .join(",");

    c = c
        .drop_query(&format!(
            "SELECT users.* FROM users \
                 WHERE users.id IN ({})",
            authors
        ))
        .await?;

    Ok((c, true))
}
