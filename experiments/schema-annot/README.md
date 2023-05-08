# Sample applications

`experiments/schema-annot/annotated` contains the schemas of a number of sample applications along with annotated versions of them which capture data ownership relationships important for compliance with privacy laws like the GDPR.

Converting and annotating these schemas required removing certain SQL features currently unsupported by k9db (a full list is provided at the top of each schema file). However, removing these features does not affect privacy compliance.

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
moderators: DATASUBJECT AND SHARDED
  domain               shards to owners               (implicitly deduced)
      via   moderators(domain) -> domains(domain)
      via   moderators(ownerHex) -> owners(ownerHex)
    with a total distance of 2
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
  user_id              shards to user_info            (implicitly deduced)
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

## Hotcrp

```
-----------------------------------------
PaperOption: SHARDED
  paperId              shards to ContactInfo          (implicitly deduced)
      via   PaperOption(paperId) -> Paper(paperId)
      via   PaperOption(leadContactId) -> ContactInfo(contactId)
    with a total distance of 2
-----------------------------------------
PaperTopic: SHARDED
  paperId              shards to ContactInfo          (explicit annotation)
      via   PaperTopic(paperId) -> Paper(paperId)
      via   PaperTopic(leadContactId) -> ContactInfo(contactId)
    with a total distance of 2
-----------------------------------------
PaperComment: SHARDED
  contactId            shards to ContactInfo          (explicit annotation)
      via   PaperComment(contactId) -> ContactInfo(contactId)
-----------------------------------------
TopicInterest: SHARDED
  contactId            shards to ContactInfo          (implicitly deduced)
      via   TopicInterest(contactId) -> ContactInfo(contactId)
-----------------------------------------
MailLog: SHARDED
  paperIds             shards to ContactInfo          (implicitly deduced)
      via   MailLog(paperIds) -> Paper(paperId)
      via   MailLog(leadContactId) -> ContactInfo(contactId)
    with a total distance of 2
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
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
  requestedBy          shards to ContactInfo          (explicit annotation)
      via   ReviewRequest(requestedBy) -> ContactInfo(contactId)
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
  contactId            shards to ContactInfo          (explicit annotation)
      via   PaperConflict(contactId) -> ContactInfo(contactId)
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
PaperReviewRefused: DATASUBJECT AND SHARDED
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
Settings: UNSHARDED
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
  follow_to            shards to users                (explicit annotation)
      via   follow_system(follow_to) -> users(id)
-----------------------------------------
favourites: SHARDED
  user                 shards to users                (implicitly deduced)
      via   favourites(user) -> users(id)
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
  post_id              shards to users                (implicitly deduced)
      via   bookmarks(post_id) -> posts(id)
      via   bookmarks(user_id) -> users(id)
    with a total distance of 2
-----------------------------------------
blocks: SHARDED
  user_id              shards to users                (implicitly deduced)
      via   blocks(user_id) -> users(id)
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
  recommend_to         shards to users                (explicit annotation)
      via   recommendations(recommend_to) -> users(id)
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
photo_albums: SHARDED
  user_id              shards to users                (explicit annotation)
      via   photo_albums(user_id) -> users(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
merit_score_points: UNSHARDED
-----------------------------------------
authentications: UNSHARDED
-----------------------------------------
follows: SHARDED
  followable_id        shards to users                (explicit annotation)
      via   follows(followable_id) -> users(id)
  follower_id          shards to users                (explicit annotation)
      via   follows(follower_id) -> users(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
merit_actions: UNSHARDED
-----------------------------------------
events: SHARDED
  user_id              shards to users                (explicit annotation)
      via   events(user_id) -> users(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
attachments: UNSHARDED
-----------------------------------------
merit_activity_logs: UNSHARDED
-----------------------------------------
votes: UNSHARDED
-----------------------------------------
badges_sashes: UNSHARDED
-----------------------------------------
activities: SHARDED
  owner_id             shards to users                (explicit annotation)
      via   activities(owner_id) -> users(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
users: DATASUBJECT
-----------------------------------------
photos: UNSHARDED
-----------------------------------------
posts: SHARDED
  user_id              shards to users                (explicit annotation)
      via   posts(user_id) -> users(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
sashes: UNSHARDED
```

## Opencart

```
-----------------------------------------
customer_reward: SHARDED
  customer_id          shards to customer             (implicitly deduced)
      via   customer_reward(customer_id) -> customer(customer_id)
-----------------------------------------
customer_online: SHARDED
  customer_id          shards to customer             (explicit annotation)
      via   customer_online(customer_id) -> customer(customer_id)
-----------------------------------------
customer_login: UNSHARDED
  [Warning] The column "email" sounds like it may belong to a data subject, but the table is not sharded. You may want to review your annotations.
-----------------------------------------
custom_field_customer_group: UNSHARDED
-----------------------------------------
customer_group: UNSHARDED
-----------------------------------------
customer_affiliate_report: SHARDED
  customer_id          shards to customer             (implicitly deduced)
      via   customer_affiliate_report(customer_id) -> customer(customer_id)
-----------------------------------------
customer_affiliate: UNSHARDED
-----------------------------------------
tax_rate_to_customer_group: UNSHARDED
-----------------------------------------
customer_activity: SHARDED
  customer_id          shards to customer             (explicit annotation)
      via   customer_activity(customer_id) -> customer(customer_id)
-----------------------------------------
currency: UNSHARDED
-----------------------------------------
api_session: UNSHARDED
-----------------------------------------
location: UNSHARDED
-----------------------------------------
customer_ip: SHARDED
  customer_id          shards to customer             (explicit annotation)
      via   customer_ip(customer_id) -> customer(customer_id)
-----------------------------------------
attribute_group: UNSHARDED
-----------------------------------------
coupon_history: SHARDED
  customer_id          shards to customer             (implicitly deduced)
      via   coupon_history(customer_id) -> customer(customer_id)
-----------------------------------------
geo_zone: UNSHARDED
-----------------------------------------
customer_history: SHARDED
  customer_id          shards to customer             (explicit annotation)
      via   customer_history(customer_id) -> customer(customer_id)
-----------------------------------------
country: UNSHARDED
-----------------------------------------
user: UNSHARDED
  [Warning] The columns "username", "password", "firstname", "lastname", "email" sound like they may belong to a data subject, but the table is not sharded. You may want to review your annotations.
-----------------------------------------
customer: DATASUBJECT
-----------------------------------------
customer_approval: SHARDED
  customer_id          shards to customer             (implicitly deduced)
      via   customer_approval(customer_id) -> customer(customer_id)
-----------------------------------------
cart: SHARDED
  customer_id          shards to customer             (implicitly deduced)
      via   cart(customer_id) -> customer(customer_id)
-----------------------------------------
user_group: UNSHARDED
-----------------------------------------
attribute_group_description: UNSHARDED
-----------------------------------------
api: UNSHARDED
  [Warning] The column "username" sounds like it may belong to a data subject, but the table is not sharded. You may want to review your annotations.
-----------------------------------------
coupon: UNSHARDED
-----------------------------------------
api_ip: UNSHARDED
-----------------------------------------
return: SHARDED
  customer_id          shards to customer             (implicitly deduced)
      via   return(customer_id) -> customer(customer_id)
-----------------------------------------
banner: UNSHARDED
-----------------------------------------
address: SHARDED
  customer_id          shards to customer             (explicit annotation)
      via   address(customer_id) -> customer(customer_id)
-----------------------------------------
category_description: UNSHARDED
-----------------------------------------
banner_image: UNSHARDED
-----------------------------------------
review: SHARDED
  customer_id          shards to customer             (implicitly deduced)
      via   review(customer_id) -> customer(customer_id)
-----------------------------------------
attribute: UNSHARDED
-----------------------------------------
category: UNSHARDED
-----------------------------------------
attribute_description: UNSHARDED
-----------------------------------------
customer_search: SHARDED
  customer_id          shards to customer             (explicit annotation)
      via   customer_search(customer_id) -> customer(customer_id)
-----------------------------------------
custom_field_value: UNSHARDED
-----------------------------------------
customer_transaction: SHARDED
  customer_id          shards to customer             (explicit annotation)
      via   customer_transaction(customer_id) -> customer(customer_id)
-----------------------------------------
order1: SHARDED
  customer_id          shards to customer             (explicit annotation)
      via   order1(customer_id) -> customer(customer_id)
-----------------------------------------
customer_wishlist: SHARDED
  customer_id          shards to customer             (implicitly deduced)
      via   customer_wishlist(customer_id) -> customer(customer_id)
-----------------------------------------
custom_field: UNSHARDED
-----------------------------------------
custom_field_description: UNSHARDED
-----------------------------------------
download: UNSHARDED
-----------------------------------------
gdpr: UNSHARDED
-----------------------------------------
order_history: UNSHARDED
-----------------------------------------
order_product: UNSHARDED
-----------------------------------------
order_recurring: UNSHARDED
-----------------------------------------
order_recurring_transaction: UNSHARDED
-----------------------------------------
order_shipment: UNSHARDED
```
