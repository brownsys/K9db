# Sample applications

`experiments/schema-annot/annotated` contains the schemas of a number of sample applications along with annotated versions of them which capture data ownership relationships important for compliance with privacy laws like the GDPR.

## Data Ownership Patterns

- `commento`: accessorship for nested comments. The `comments` table contains a `parentHex` with a self referencing foreign key.
- `ghchat`: groups have admins, so each member of a group only has access to the group such that chat messages are owned exclusively by their sender and only accessed by a member of a group.
- `instagram`: messages/conversations are `OWNED_BY` both recipient and sender, shares are `ACCESSED_BY` the user being shared with, group membership is `ACCESSED_BY` the person who adds members
- `schnack`: each comment is `OWNED_BY` a single user.
- `hotcrp`: Authors should see a `PaperReview`, but not who conducted the review. Hence fields related to who requested the review (`requestedBy`) should be anonymized when the author `GDPR GET`s their data.
- `socify`: follows are dually `OWNED_BY` follower and followable users.
- `mouthful`: `Author` column represents a data subject embedded within the `Comment` table. Unlike other schemas, there is no separate table that clearly refers to a data subject.

## Annotations Used

- `commento`: 3 `DATA_SUBJECT`, 8 `OWNED_BY`, 3 `ACCESSED_BY`, 2 `ON GET ANON`, 1 `ON DEL ANON`, 1 `ON DEL DELETE_ROW`
- `ghchat`: 1 `DATA_SUBJECT`, 7 `OWNED_BY`, 2 `ACCESSED_BY`, 1 `ACCESSES`,
- `instagram`: 1 `DATA_SUBJECT`, 17 `OWNED_BY`, 2 `ACCESSED_BY`
- `schnack`: 1 `DATA_SUBJECT`, 1 `OWNED_BY`
- `hotcrp`: 2 `DATA_SUBJECT`, 14 `OWNED_BY`, 9 `ACCESSED_BY`, 4 `ON GET ANON`, 2 `ON DEL ANON`
- `socify`: 1 `DATA_SUBJECT`, 8 `OWNED_BY`
- `mouthful`: 1 `DATA_SUBJECT`, 1 `OWNED_BY`

## Unsupported Features

Converting and annotating these schemas required removing certain SQL features currently unsupported by k9db. However, removing these features does not affect privacy compliance. A list of unsupported features is provided below:

- `all schemas`: reserved keywords as column or table names (e.g. groups, user, match, index, key, from), multi-column primary keys, `[UNIQUE] KEY`, `CREATE TABLE IF NOT EXISTS`, `DEFAULT` values, certain column type definitions (e.g. timestamp, boolean, varchar, tinyint), `AUTO_INCREMENT`

Specific schemas:

- `commento`: self referencing foreign keys with `ACCESSED_BY` for `parentHex` in the `comments` table are unsupported (issue #170)
- `mouthful`: `Author` column representing a data subject embedded within the `Comment` table. As a work around, we add a separate table for users.

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

## Prestashop

```
-----------------------------------------
PREFIX_profile: UNSHARDED
-----------------------------------------
PREFIX_order_slip_detail_tax: UNSHARDED
-----------------------------------------
PREFIX_product_sale: UNSHARDED
-----------------------------------------
PREFIX_configuration_kpi: UNSHARDED
-----------------------------------------
PREFIX_date_range: UNSHARDED
-----------------------------------------
PREFIX_feature_shop: UNSHARDED
-----------------------------------------
PREFIX_profile_lang: UNSHARDED
-----------------------------------------
PREFIX_smarty_last_flush: UNSHARDED
-----------------------------------------
PREFIX_product_download: UNSHARDED
-----------------------------------------
PREFIX_product_attribute_image: UNSHARDED
-----------------------------------------
PREFIX_carrier_lang: UNSHARDED
-----------------------------------------
PREFIX_stock_mvt_reason_lang: UNSHARDED
-----------------------------------------
PREFIX_product_attribute: UNSHARDED
-----------------------------------------
PREFIX_cart_rule_combination: UNSHARDED
-----------------------------------------
PREFIX_quick_access_lang: UNSHARDED
-----------------------------------------
PREFIX_page_viewed: UNSHARDED
-----------------------------------------
PREFIX_cart_rule_product_rule_value: UNSHARDED
-----------------------------------------
PREFIX_page_type: UNSHARDED
-----------------------------------------
PREFIX_customized_data: UNSHARDED
-----------------------------------------
PREFIX_pack: UNSHARDED
-----------------------------------------
PREFIX_configuration: UNSHARDED
-----------------------------------------
PREFIX_order_state_lang: UNSHARDED
-----------------------------------------
PREFIX_order_return: SHARDED
  id_customer          shards to PREFIX_customer      (implicitly deduced)
      via   PREFIX_order_return(id_customer) -> PREFIX_customer(id_customer)
-----------------------------------------
PREFIX_module_currency: UNSHARDED
-----------------------------------------
PREFIX_supply_order_state: UNSHARDED
-----------------------------------------
PREFIX_order_slip_detail: UNSHARDED
-----------------------------------------
PREFIX_stock_available: UNSHARDED
-----------------------------------------
PREFIX_order_slip: UNSHARDED
-----------------------------------------
PREFIX_supplier_lang: UNSHARDED
-----------------------------------------
PREFIX_order_message_lang: UNSHARDED
-----------------------------------------
PREFIX_supplier: UNSHARDED
-----------------------------------------
PREFIX_order_invoice_tax: UNSHARDED
-----------------------------------------
PREFIX_order_invoice: UNSHARDED
-----------------------------------------
PREFIX_product_attribute_combination: UNSHARDED
-----------------------------------------
PREFIX_mail: UNSHARDED
-----------------------------------------
PREFIX_module_carrier: UNSHARDED
-----------------------------------------
PREFIX_module_preference: UNSHARDED
-----------------------------------------
PREFIX_module_group: UNSHARDED
-----------------------------------------
PREFIX_product: UNSHARDED
-----------------------------------------
PREFIX_page: UNSHARDED
-----------------------------------------
PREFIX_orders: UNSHARDED
-----------------------------------------
PREFIX_module_country: UNSHARDED
-----------------------------------------
PREFIX_order_state: UNSHARDED
  [Warning] The column "send_email" sounds like it may belong to a data subject, but the table is not sharded. You may want to review your annotations.
-----------------------------------------
PREFIX_module_access: UNSHARDED
-----------------------------------------
PREFIX_module: UNSHARDED
-----------------------------------------
PREFIX_authorization_role: UNSHARDED
-----------------------------------------
PREFIX_order_cart_rule: UNSHARDED
-----------------------------------------
PREFIX_order_history: UNSHARDED
-----------------------------------------
PREFIX_meta_lang: UNSHARDED
-----------------------------------------
PREFIX_order_detail_tax: UNSHARDED
-----------------------------------------
PREFIX_meta: UNSHARDED
-----------------------------------------
PREFIX_manufacturer: UNSHARDED
-----------------------------------------
PREFIX_state: UNSHARDED
-----------------------------------------
PREFIX_specific_price_rule_condition: UNSHARDED
-----------------------------------------
PREFIX_order_message: UNSHARDED
-----------------------------------------
PREFIX_country: UNSHARDED
-----------------------------------------
PREFIX_order_carrier: UNSHARDED
-----------------------------------------
PREFIX_message_readed: UNSHARDED
-----------------------------------------
PREFIX_image_type: UNSHARDED
-----------------------------------------
PREFIX_contact_shop: UNSHARDED
-----------------------------------------
PREFIX_product_shop: UNSHARDED
-----------------------------------------
PREFIX_image_lang: UNSHARDED
-----------------------------------------
PREFIX_product_lang: UNSHARDED
-----------------------------------------
PREFIX_connections: UNSHARDED
-----------------------------------------
PREFIX_hook_module_exceptions: UNSHARDED
-----------------------------------------
PREFIX_hook_module: UNSHARDED
-----------------------------------------
PREFIX_message: UNSHARDED
-----------------------------------------
PREFIX_cms_category: UNSHARDED
-----------------------------------------
PREFIX_cms_category_shop: UNSHARDED
-----------------------------------------
PREFIX_tax: UNSHARDED
-----------------------------------------
PREFIX_guest: SHARDED
  id_customer          shards to PREFIX_customer      (explicit annotation)
      via   PREFIX_guest(id_customer) -> PREFIX_customer(id_customer)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
PREFIX_group_lang: UNSHARDED
-----------------------------------------
PREFIX_customization_field: UNSHARDED
-----------------------------------------
PREFIX_group: UNSHARDED
-----------------------------------------
PREFIX_log: UNSHARDED
-----------------------------------------
PREFIX_image_shop: UNSHARDED
-----------------------------------------
PREFIX_gender_lang: UNSHARDED
  [Warning] The column "id_gender" sounds like it may belong to a data subject, but the table is not sharded. You may want to review your annotations.
-----------------------------------------
PREFIX_request_sql: UNSHARDED
-----------------------------------------
PREFIX_store_shop: UNSHARDED
-----------------------------------------
PREFIX_feature_value_lang: UNSHARDED
-----------------------------------------
PREFIX_cart_rule_product_rule: UNSHARDED
-----------------------------------------
PREFIX_feature_product: UNSHARDED
-----------------------------------------
PREFIX_cart_rule_country: UNSHARDED
-----------------------------------------
PREFIX_cart: UNSHARDED
-----------------------------------------
PREFIX_group_reduction: UNSHARDED
-----------------------------------------
PREFIX_carrier_zone: UNSHARDED
-----------------------------------------
PREFIX_carrier: UNSHARDED
-----------------------------------------
PREFIX_attribute_impact: UNSHARDED
-----------------------------------------
PREFIX_quick_access: UNSHARDED
-----------------------------------------
PREFIX_category_lang: UNSHARDED
-----------------------------------------
PREFIX_tax_rule: UNSHARDED
-----------------------------------------
PREFIX_product_carrier: UNSHARDED
-----------------------------------------
PREFIX_timezone: UNSHARDED
-----------------------------------------
PREFIX_employee_shop: UNSHARDED
-----------------------------------------
PREFIX_category: UNSHARDED
-----------------------------------------
PREFIX_manufacturer_lang: UNSHARDED
-----------------------------------------
PREFIX_cart_rule_shop: UNSHARDED
-----------------------------------------
PREFIX_product_supplier: UNSHARDED
-----------------------------------------
PREFIX_order_return_state_lang: UNSHARDED
-----------------------------------------
PREFIX_order_return_detail: UNSHARDED
-----------------------------------------
PREFIX_cart_rule_group: UNSHARDED
-----------------------------------------
PREFIX_currency: UNSHARDED
-----------------------------------------
PREFIX_cart_cart_rule: UNSHARDED
-----------------------------------------
PREFIX_attachment_lang: UNSHARDED
-----------------------------------------
PREFIX_customer_message: UNSHARDED
-----------------------------------------
PREFIX_hook_alias: UNSHARDED
-----------------------------------------
PREFIX_attachment: UNSHARDED
-----------------------------------------
PREFIX_feature_value: UNSHARDED
-----------------------------------------
PREFIX_employee: DATASUBJECT
-----------------------------------------
PREFIX_order_return_state: UNSHARDED
-----------------------------------------
PREFIX_contact: DATASUBJECT
-----------------------------------------
PREFIX_operating_system: UNSHARDED
-----------------------------------------
PREFIX_feature_lang: UNSHARDED
-----------------------------------------
PREFIX_cart_product: UNSHARDED
-----------------------------------------
PREFIX_cms: UNSHARDED
-----------------------------------------
PREFIX_cart_rule_carrier: UNSHARDED
-----------------------------------------
PREFIX_accessory: UNSHARDED
-----------------------------------------
PREFIX_category_group: UNSHARDED
-----------------------------------------
PREFIX_risk: UNSHARDED
-----------------------------------------
PREFIX_alias: UNSHARDED
-----------------------------------------
PREFIX_hook: UNSHARDED
-----------------------------------------
PREFIX_delivery: UNSHARDED
-----------------------------------------
PREFIX_product_attachment: UNSHARDED
-----------------------------------------
PREFIX_image: UNSHARDED
-----------------------------------------
PREFIX_cart_rule: UNSHARDED
-----------------------------------------
PREFIX_connections_page: UNSHARDED
-----------------------------------------
PREFIX_country_lang: UNSHARDED
-----------------------------------------
PREFIX_feature: UNSHARDED
-----------------------------------------
PREFIX_cart_rule_lang: UNSHARDED
-----------------------------------------
PREFIX_customer_session: SHARDED
  id_customer          shards to PREFIX_customer      (implicitly deduced)
      via   PREFIX_customer_session(id_customer) -> PREFIX_customer(id_customer)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
PREFIX_order_detail: UNSHARDED
-----------------------------------------
PREFIX_store: UNSHARDED
  [Warning] The column "email" sounds like it may belong to a data subject, but the table is not sharded. You may want to review your annotations.
-----------------------------------------
PREFIX_stock: UNSHARDED
-----------------------------------------
PREFIX_cms_category_lang: UNSHARDED
-----------------------------------------
PREFIX_cart_rule_product_rule_group: UNSHARDED
-----------------------------------------
PREFIX_configuration_kpi_lang: UNSHARDED
-----------------------------------------
PREFIX_contact_lang: UNSHARDED
-----------------------------------------
PREFIX_address: UNSHARDED
  [Warning] The columns "lastname", "firstname" sound like they may belong to a data subject, but the table is not sharded. You may want to review your annotations.
-----------------------------------------
PREFIX_configuration_lang: UNSHARDED
-----------------------------------------
PREFIX_referrer_cache: UNSHARDED
-----------------------------------------
PREFIX_risk_lang: UNSHARDED
-----------------------------------------
PREFIX_tab_module_preference: UNSHARDED
-----------------------------------------
PREFIX_product_attribute_shop: UNSHARDED
-----------------------------------------
PREFIX_connections_source: UNSHARDED
-----------------------------------------
PREFIX_currency_lang: UNSHARDED
-----------------------------------------
PREFIX_access: UNSHARDED
-----------------------------------------
PREFIX_customer: DATASUBJECT
-----------------------------------------
PREFIX_customization_field_lang: UNSHARDED
-----------------------------------------
PREFIX_carrier_group: UNSHARDED
-----------------------------------------
PREFIX_product_tag: UNSHARDED
-----------------------------------------
PREFIX_order_payment: UNSHARDED
-----------------------------------------
PREFIX_customer_group: SHARDED
  id_customer          shards to PREFIX_customer      (implicitly deduced)
      via   PREFIX_customer_group(id_customer) -> PREFIX_customer(id_customer)
-----------------------------------------
PREFIX_webservice_permission: UNSHARDED
-----------------------------------------
PREFIX_category_product: UNSHARDED
-----------------------------------------
PREFIX_customer_message_sync_imap: UNSHARDED
-----------------------------------------
PREFIX_product_group_reduction_cache: UNSHARDED
-----------------------------------------
PREFIX_memcached_servers: UNSHARDED
-----------------------------------------
PREFIX_customer_thread: SHARDED
  id_customer          shards to PREFIX_customer      (implicitly deduced)
      via   PREFIX_customer_thread(id_customer) -> PREFIX_customer(id_customer)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
PREFIX_customization: UNSHARDED
-----------------------------------------
PREFIX_cms_lang: UNSHARDED
-----------------------------------------
PREFIX_gender: UNSHARDED
  [Warning] The column "id_gender" sounds like it may belong to a data subject, but the table is not sharded. You may want to review your annotations.
-----------------------------------------
PREFIX_range_price: UNSHARDED
-----------------------------------------
PREFIX_range_weight: UNSHARDED
-----------------------------------------
PREFIX_referrer: UNSHARDED
-----------------------------------------
PREFIX_search_word: UNSHARDED
-----------------------------------------
PREFIX_referrer_shop: UNSHARDED
-----------------------------------------
PREFIX_webservice_account_shop: UNSHARDED
-----------------------------------------
PREFIX_search_engine: UNSHARDED
-----------------------------------------
PREFIX_search_index: UNSHARDED
-----------------------------------------
PREFIX_specific_price: UNSHARDED
-----------------------------------------
PREFIX_tag: UNSHARDED
-----------------------------------------
PREFIX_tag_count: UNSHARDED
-----------------------------------------
PREFIX_tax_lang: UNSHARDED
-----------------------------------------
PREFIX_web_browser: UNSHARDED
-----------------------------------------
PREFIX_zone: UNSHARDED
-----------------------------------------
PREFIX_store_lang: UNSHARDED
-----------------------------------------
PREFIX_webservice_account: UNSHARDED
-----------------------------------------
PREFIX_required_field: UNSHARDED
-----------------------------------------
PREFIX_product_country_tax: UNSHARDED
-----------------------------------------
PREFIX_tax_rules_group: UNSHARDED
-----------------------------------------
PREFIX_specific_price_priority: UNSHARDED
-----------------------------------------
PREFIX_import_match: UNSHARDED
-----------------------------------------
PREFIX_shop_url: UNSHARDED
-----------------------------------------
PREFIX_country_shop: UNSHARDED
-----------------------------------------
PREFIX_carrier_shop: UNSHARDED
-----------------------------------------
PREFIX_specific_price_rule: UNSHARDED
-----------------------------------------
PREFIX_address_format: UNSHARDED
-----------------------------------------
PREFIX_cms_shop: UNSHARDED
-----------------------------------------
PREFIX_currency_shop: UNSHARDED
-----------------------------------------
PREFIX_group_shop: UNSHARDED
-----------------------------------------
PREFIX_tax_rules_group_shop: UNSHARDED
-----------------------------------------
PREFIX_zone_shop: UNSHARDED
-----------------------------------------
PREFIX_manufacturer_shop: UNSHARDED
-----------------------------------------
PREFIX_supplier_shop: UNSHARDED
-----------------------------------------
PREFIX_module_shop: UNSHARDED
-----------------------------------------
PREFIX_stock_mvt_reason: UNSHARDED
-----------------------------------------
PREFIX_warehouse: UNSHARDED
-----------------------------------------
PREFIX_warehouse_product_location: UNSHARDED
-----------------------------------------
PREFIX_warehouse_shop: UNSHARDED
-----------------------------------------
PREFIX_warehouse_carrier: UNSHARDED
-----------------------------------------
PREFIX_supply_order: UNSHARDED
-----------------------------------------
PREFIX_supply_order_detail: UNSHARDED
-----------------------------------------
PREFIX_supply_order_state_lang: UNSHARDED
-----------------------------------------
PREFIX_supply_order_history: UNSHARDED
  [Warning] The columns "employee_lastname", "employee_firstname" sound like they may belong to a data subject, but the table is not sharded. You may want to review your annotations.
-----------------------------------------
PREFIX_supply_order_receipt_history: UNSHARDED
  [Warning] The columns "employee_lastname", "employee_firstname" sound like they may belong to a data subject, but the table is not sharded. You may want to review your annotations.
-----------------------------------------
PREFIX_specific_price_rule_condition_group: UNSHARDED
-----------------------------------------
PREFIX_category_shop: UNSHARDED
-----------------------------------------
PREFIX_carrier_tax_rules_group_shop: UNSHARDED
-----------------------------------------
PREFIX_order_invoice_payment: UNSHARDED
-----------------------------------------
PREFIX_cms_role: UNSHARDED
-----------------------------------------
PREFIX_smarty_cache: UNSHARDED
-----------------------------------------
PREFIX_smarty_lazy_cache: UNSHARDED
-----------------------------------------
PREFIX_cms_role_lang: UNSHARDED
-----------------------------------------
PREFIX_employee_session: UNSHARDED
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
