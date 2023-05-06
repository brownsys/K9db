# Sample applications

`experiments/schema-annot/annotated` contains the schemas of a number of sample applications along with annotated versions of them which capture data ownership relationships important for compliance with privacy laws like the GDPR.

## Data Ownership Patterns

- `commento`: accessorship for nested comments. The `comments` table contains a `parentHex` with a self referencing foreign key.
- `ghchat`: groups have admins, so each member of a group only has access to the group such that chat messages are owned exclusively by their sender and only accessed by a member of a group.
- `instagram`: messages/conversations are `OWNED_BY` both recipient and sender, shares are `ACCESSED_BY` the user being shared with, group membership is `ACCESSED_BY` the person who adds members
- `schnack`: each comment is `OWNED_BY` a single user.
- `hotcrp`: Authors should see a `PaperReview`, but not who conducted the review. Hence fields related to who requested the review (`requestedBy`) should be anonymized when the author `GDPR GET`s their data.
- `mouthful`: `Author` column represents a data subject embedded within the `Comment` table. Unlike other schemas, there is no separate table that clearly refers to a data subject.

## Annotations Used

- `commento`: 3 `DATA_SUBJECT`, 8 `OWNED_BY`, 3 `ACCESSED_BY`, 2 `ON GET ANON`, 1 `ON DEL DELETE_ROW`
- `ghchat`: 1 `DATA_SUBJECT`, 7 `OWNED_BY`, 1 `ACCESSED_BY`, 1 `ACCESSES`,
- `instagram`: 1 `DATA_SUBJECT`, 18 `OWNED_BY`, 1 `ACCESSED_BY`
- `schnack`: 1 `DATA_SUBJECT`, 1 `OWNED_BY`
- `hotcrp`: 2 `DATA_SUBJECT`, 14 `OWNED_BY`, 9 `ACCESSED_BY`, 4 `ON GET ANON`, 2 `ON DEL ANON`
- `socify`: 1 `DATA_SUBJECT`, 10 `OWNED_BY`
- `mouthful`: 1 `DATA_SUBJECT`, 1 `OWNED_BY`

## Unsupported Features

Converting and annotating these schemas required removing certain SQL features currently unsupported by k9db. However, removing these features does not affect privacy compliance. A list of unsupported features is provided below:

- `all schemas`: reserved keywords as column or table names (e.g. groups, user, match, index, key, from), multi-column primary keys, `[UNIQUE] KEY`, `CREATE TABLE IF NOT EXISTS`, `DEFAULT` values, certain column type definitions (e.g. timestamp, boolean, varchar, tinyint), `AUTO_INCREMENT`

Specific schemas:

- `commento`: self referencing foreign keys with `ACCESSED_BY` for `parentHex` in the `comments` table are unsupported (issue #170)
- `mouthful`: `Author` column representing a data subject embedded within the `Comment` table. As a work around, we add a separate table for users (see issue #171).

# Explain Compliance

The following shows the output of running k9db's `EXPLAIN COMPLIANCE` command on each schema.

## Commento

```
-----------------------------------------
config: UNSHARDED
-----------------------------------------
owners: DATASUBJECT
-----------------------------------------
ownerSessions: SHARDED
  ownerHex             shards to owners               (explicit annotation)
      via   ownerSessions(ownerHex) -> owners(ownerHex)
-----------------------------------------
commenterSessions: SHARDED
  commenterHex         shards to commenters           (explicit annotation)
      via   commenterSessions(commenterHex) -> commenters(commenterHex)
-----------------------------------------
ownerConfirmHexes: SHARDED
  ownerHex             shards to owners               (explicit annotation)
      via   ownerConfirmHexes(ownerHex) -> owners(ownerHex)
-----------------------------------------
ownerResetHexes: SHARDED
  ownerHex             shards to owners               (explicit annotation)
      via   ownerResetHexes(ownerHex) -> owners(ownerHex)
-----------------------------------------
domains: SHARDED
  ownerHex             shards to owners               (explicit annotation)
      via   domains(ownerHex) -> owners(ownerHex)
-----------------------------------------
moderators: DATASUBJECT
-----------------------------------------
commenters: DATASUBJECT
-----------------------------------------
comments: SHARDED
  commenterHex         shards to commenters           (explicit annotation)
      via   comments(commenterHex) -> commenters(commenterHex)
-----------------------------------------
votes: SHARDED
  commenterHex         shards to commenters           (explicit annotation)
      via   votes(commenterHex) -> commenters(commenterHex)
-----------------------------------------
views: SHARDED
  commenterHex         shards to commenters           (explicit annotation)
      via   views(commenterHex) -> commenters(commenterHex)
```

## ghchat

```
-----------------------------------------
user_info: DATASUBJECT
-----------------------------------------
group_msg: SHARDED
  from_user            shards to user_info            (explicit annotation)
      via   group_msg(from_user) -> user_info(id)
-----------------------------------------
group_info: SHARDED
  creator_id           shards to user_info            (explicit annotation)
      via   group_info(creator_id) -> user_info(id)
-----------------------------------------
group_user_relation: SHARDED
  user_id              shards to user_info            (explicit annotation)
      via   group_user_relation(user_id) -> user_info(id)
-----------------------------------------
private_msg: SHARDED
  from_user            shards to user_info            (explicit annotation)
      via   private_msg(from_user) -> user_info(id)
  to_user              shards to user_info            (explicit annotation)
      via   private_msg(to_user) -> user_info(id)
-----------------------------------------
user_user_relation: SHARDED
  user_id              shards to user_info            (explicit annotation)
      via   user_user_relation(user_id) -> user_info(id)
  from_user            shards to user_info            (explicit annotation)
      via   user_user_relation(from_user) -> user_info(id)
```

## Instagram

```
-----------------------------------------
likes: SHARDED
  like_by              shards to users                (explicit annotation)
      via   likes(like_by) -> users(id)
-----------------------------------------
hashtags: SHARDED
  user_id              shards to users                (explicit annotation)
      via   hashtags(user_id) -> users(id)
-----------------------------------------
messages: SHARDED
  mssg_by              shards to users                (explicit annotation)
      via   messages(mssg_by) -> users(id)
  mssg_to              shards to users                (explicit annotation)
      via   messages(mssg_to) -> users(id)
-----------------------------------------
group_members: SHARDED
  member               shards to users                (explicit annotation)
      via   group_members(member) -> users(id)
-----------------------------------------
follow_system: SHARDED
  follow_by            shards to users                (explicit annotation)
      via   follow_system(follow_by) -> users(id)
-----------------------------------------
favourites: SHARDED
  fav_by               shards to users                (explicit annotation)
      via   favourites(fav_by) -> users(id)
-----------------------------------------
groups1: SHARDED
  admin                shards to users                (implicitly deduced)
      via   groups1(admin) -> users(id)
-----------------------------------------
comments: SHARDED
  comment_by           shards to users                (explicit annotation)
      via   comments(comment_by) -> users(id)
-----------------------------------------
conversations: SHARDED
  user_one             shards to users                (explicit annotation)
      via   conversations(user_one) -> users(id)
  user_two             shards to users                (explicit annotation)
      via   conversations(user_two) -> users(id)
-----------------------------------------
bookmarks: SHARDED
  bkmrk_by             shards to users                (explicit annotation)
      via   bookmarks(bkmrk_by) -> users(id)
-----------------------------------------
blocks: SHARDED
  block_by             shards to users                (explicit annotation)
      via   blocks(block_by) -> users(id)
-----------------------------------------
posts: SHARDED
  user_id              shards to users                (explicit annotation)
      via   posts(user_id) -> users(id)
-----------------------------------------
users: DATASUBJECT
-----------------------------------------
notifications: SHARDED
  notify_to            shards to users                (explicit annotation)
      via   notifications(notify_to) -> users(id)
-----------------------------------------
post_tags: SHARDED
  user                 shards to users                (explicit annotation)
      via   post_tags(user) -> users(id)
-----------------------------------------
profile_views: SHARDED
  view_by              shards to users                (explicit annotation)
      via   profile_views(view_by) -> users(id)
-----------------------------------------
tags: SHARDED
  user                 shards to users                (implicitly deduced)
      via   tags(user) -> users(id)
-----------------------------------------
recommendations: SHARDED
  recommend_by         shards to users                (explicit annotation)
      via   recommendations(recommend_by) -> users(id)
-----------------------------------------
shares: SHARDED
  share_by             shards to users                (explicit annotation)
      via   shares(share_by) -> users(id)
```

## Schnack

```
-----------------------------------------
user: DATASUBJECT
-----------------------------------------
subscription: UNSHARDED
-----------------------------------------
comment: SHARDED
  user_id              shards to user                 (explicit annotation)
      via   comment(user_id) -> user(id)
-----------------------------------------
setting: UNSHARDED
-----------------------------------------
oauth_provider: UNSHARDED
```

## Hotcrp

```
-----------------------------------------
PaperAuthors: SHARDED
  contactId            shards to ContactInfo          (explicit annotation)
      via   PaperAuthors(contactId) -> ContactInfo(contactId)
-----------------------------------------
PaperComment: SHARDED
  contactId            shards to ContactInfo          (explicit annotation)
      via   PaperComment(contactId) -> ContactInfo(contactId)
-----------------------------------------
TopicInterest: SHARDED
  contactId            shards to ContactInfo          (implicitly deduced)
      via   TopicInterest(contactId) -> ContactInfo(contactId)
-----------------------------------------
MailLog: UNSHARDED
  [Warning] The column "emailBody" sounds like it may belong to a data subject, but the table is not sharded. You may want to review your annotations.
-----------------------------------------
FilteredDocument: SHARDED
  inDocId              shards to ContactInfo          (explicit annotation)
      via   FilteredDocument(inDocId) -> DocumentLink(documentId)
      via   FilteredDocument(paperId) -> Paper(paperId)
      via   FilteredDocument(leadContactId) -> ContactInfo(contactId)
    with a total distance of 3
-----------------------------------------
PaperWatch: SHARDED
  contactId            shards to ContactInfo          (explicit annotation)
      via   PaperWatch(contactId) -> ContactInfo(contactId)
-----------------------------------------
DeletedContactInfo: SHARDED
  contactId            shards to ContactInfo          (explicit annotation)
      via   DeletedContactInfo(contactId) -> ContactInfo(contactId)
-----------------------------------------
DocumentLink: SHARDED
  paperId              shards to ContactInfo          (implicitly deduced)
      via   DocumentLink(paperId) -> Paper(paperId)
      via   DocumentLink(leadContactId) -> ContactInfo(contactId)
    with a total distance of 2
-----------------------------------------
Capability: SHARDED
  contactId            shards to ContactInfo          (explicit annotation)
      via   Capability(contactId) -> ContactInfo(contactId)
-----------------------------------------
ActionLog: SHARDED
  contactId            shards to ContactInfo          (explicit annotation)
      via   ActionLog(contactId) -> ContactInfo(contactId)
-----------------------------------------
ReviewRequest: DATASUBJECT AND SHARDED
  paperId              shards to ContactInfo          (implicitly deduced)
      via   ReviewRequest(paperId) -> Paper(paperId)
      via   ReviewRequest(leadContactId) -> ContactInfo(contactId)
    with a total distance of 2
-----------------------------------------
Paper: SHARDED
  leadContactId        shards to ContactInfo          (explicit annotation)
      via   Paper(leadContactId) -> ContactInfo(contactId)
-----------------------------------------
PaperReview: SHARDED
  contactId            shards to ContactInfo          (explicit annotation)
      via   PaperReview(contactId) -> ContactInfo(contactId)
-----------------------------------------
ReviewRating: SHARDED
  contactId            shards to ContactInfo          (explicit annotation)
      via   ReviewRating(contactId) -> ContactInfo(contactId)
-----------------------------------------
PaperConflict: SHARDED
  paperId              shards to ContactInfo          (implicitly deduced)
      via   PaperConflict(paperId) -> Paper(paperId)
      via   PaperConflict(leadContactId) -> ContactInfo(contactId)
    with a total distance of 2
-----------------------------------------
PaperStorage: UNSHARDED
-----------------------------------------
Formula: SHARDED
  createdBy            shards to ContactInfo          (explicit annotation)
      via   Formula(createdBy) -> ContactInfo(contactId)
-----------------------------------------
ContactInfo: DATASUBJECT
-----------------------------------------
PaperReviewPreference: SHARDED
  contactId            shards to ContactInfo          (explicit annotation)
      via   PaperReviewPreference(contactId) -> ContactInfo(contactId)
-----------------------------------------
PaperReviewRefused: SHARDED
  contactId            shards to ContactInfo          (explicit annotation)
      via   PaperReviewRefused(contactId) -> ContactInfo(contactId)
-----------------------------------------
PaperTag: SHARDED
  paperId              shards to ContactInfo          (implicitly deduced)
      via   PaperTag(paperId) -> Paper(paperId)
      via   PaperTag(leadContactId) -> ContactInfo(contactId)
    with a total distance of 2
-----------------------------------------
TopicArea: UNSHARDED
-----------------------------------------
PaperTagAnno: UNSHARDED
-----------------------------------------
PaperTopic: SHARDED
  paperId              shards to ContactInfo          (explicit annotation)
      via   PaperTopic(paperId) -> Paper(paperId)
      via   PaperTopic(leadContactId) -> ContactInfo(contactId)
    with a total distance of 2
-----------------------------------------
Settings: UNSHARDED
```

## Socify

```
-----------------------------------------
merit_scores: UNSHARDED
-----------------------------------------
event_attendees: SHARDED
  user_id              shards to users                (explicit annotation)
      via   event_attendees(user_id) -> users(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
merit_score_points: UNSHARDED
-----------------------------------------
authentications: SHARDED
  user_id              shards to users                (explicit annotation)
      via   authentications(user_id) -> users(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
follows: SHARDED
  follower_id          shards to users                (explicit annotation)
      via   follows(follower_id) -> users(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
merit_actions: SHARDED
  user_id              shards to users                (explicit annotation)
      via   merit_actions(user_id) -> users(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
events: SHARDED
  user_id              shards to users                (explicit annotation)
      via   events(user_id) -> users(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
sashes: UNSHARDED
-----------------------------------------
attachments: UNSHARDED
-----------------------------------------
merit_activity_logs: SHARDED
  action_id            shards to users                (explicit annotation)
      via   merit_activity_logs(action_id) -> merit_actions(id)
      via   merit_activity_logs(user_id) -> users(id)
    with a total distance of 2
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
votes: SHARDED
  voter_id             shards to users                (implicitly deduced)
      via   votes(voter_id) -> users(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
badges_sashes: SHARDED
  notified_user        shards to users                (implicitly deduced)
      via   badges_sashes(notified_user) -> users(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
activities: SHARDED
  owner_id             shards to users                (explicit annotation)
      via   activities(owner_id) -> users(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
users: DATASUBJECT
-----------------------------------------
photo_albums: SHARDED
  user_id              shards to users                (explicit annotation)
      via   photo_albums(user_id) -> users(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
photos: SHARDED
  photo_album_id       shards to users                (implicitly deduced)
      via   photos(photo_album_id) -> photo_albums(id)
      via   photos(user_id) -> users(id)
    with a total distance of 2
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
posts: SHARDED
  user_id              shards to users                (explicit annotation)
      via   posts(user_id) -> users(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
```

## Mouthful

```
-----------------------------------------
user: DATASUBJECT
-----------------------------------------
Thread: UNSHARDED
-----------------------------------------
Comment: SHARDED
  Author               shards to user                 (explicit annotation)
      via   Comment(Author) -> user(id)
```
