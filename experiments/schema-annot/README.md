# Explain Compliance

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
