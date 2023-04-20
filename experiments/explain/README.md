# EXPLAIN Scenarios

## How to Run

1. Start the proxy

```
   bazel run //:pelton
```

2. Run the schema file

```
mariadb --port=10001 --host=127.0.0.1 < experiments/explain/shuup/{schema_name}.sql
```

## Summary

Which table is the data subject (PII) - `shuup_contact`, `auth_user` or `shuup_personcontact`?

0. Run `EXPLAIN` on our base, unannotated schema.

1. Annotate `auth_user` as PII: guest users are `personcontact`s ⇒ they have addresses etc. but they would not be sharded, because `personcontact.user_id` column is nullable. Explain tells us that there may be no owner in some cases. Explain also warns us that `contact` has `email` etc fields but is unowned => Forgot `OWNS` on `contact_ptr_id`

2. `auth_user` and `contact` are PII: because explain flagged the nullable `personcontact.user_id` ⇒ `OWNS` into PII creates warning

3. `contact` is PII: overcompliant. The developer will figure this out by looking at how things are sharded.

4. `shuup_personcontact` is PII.

## Extract from paper discussing Shuup

Shuup’s schema has several tables that might correspond to data subjects. Pelton’s `EXPLAIN COMPLIANCE` helps the developer understand that they need to annotate `personcontact`. An incompliant (but plausible) alternative would be to annotate `auth_user`, the login details table. This results in `contact` being unconnected to the ownership graph, as there are no foreign keys `auth_user`. The `personcontact` table has such a foreign key, but it is nullable (e.g., for guest customers who lack accounts), and thus some of its rows will be stored in μDBs and others in the orphaned region. Pelton’s `EXPLAIN COMPLIANCE` helps the developer identify and rectify these issues:

```
1 Table "contact": GLOBAL
2 [Compliance Warning] Column "email" suggests personal
data, but the table is not connected to any owners. 3 Table "personcontact": in μDB for auth_user.id
4 [Compliance Warning] Table has owners, but nullable
       foreign key may make data unreachable.
```

A developer might also annotate `contact` with `DATA SUBJECT`, but that table includes entries for customers and companies. Annotating it makes companies into data subjects, will duplicate various company-related tables across μDBs. `EXPLAIN COMPLIANCE` alerts the developer to this as well.

## Step by Step

### 0. No PII (starting point)

Schema in `shuup/0-no-pii.sql`

Explain identifies our candidate data subjects: auth_user, shuup_contact, shuup_personcontact.

```
############# EXPLAIN COMPLIANCE #############
-----------------------------------------
auth_user: UNSHARDED
  [Warning] The columns "username", "password" sound like they may belong to a data subject, but the table is not sharded. You may want to review your annotations.
-----------------------------------------
shuup_contact: UNSHARDED
  [Warning] The column "email" sounds like it may belong to a data subject, but the table is not sharded. You may want to review your annotations.
-----------------------------------------
shuup_personcontact: UNSHARDED
  [Warning] The column "email" sounds like it may belong to a data subject, but the table is not sharded. You may want to review your annotations.
-----------------------------------------
shuup_gdpr_gdpruserconsent: UNSHARDED
-----------------------------------------
shuup_companycontact: UNSHARDED
-----------------------------------------
shuup_companycontact_members: UNSHARDED
-----------------------------------------
shuup_shop: UNSHARDED
############# END EXPLAIN COMPLIANCE #############
```

### 1. `auth_user` is PII

Schema in `shuup/1-auth-user-is-pii.sql`

```
-----------------------------------------
auth_user: DATASUBJECT
-----------------------------------------
shuup_contact: UNSHARDED
  [Warning] The column "email" sounds like it may belong to a data subject, but the table is not sharded. You may want to review your annotations.
-----------------------------------------
shuup_personcontact: SHARDED
  user_id              shards to auth_user            (implicitly deduced)
      via   shuup_personcontact(user_id) -> auth_user(id)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
shuup_gdpr_gdpruserconsent: SHARDED
  user_id              shards to auth_user            (explicit annotation)
      via   shuup_gdpr_gdpruserconsent(user_id) -> auth_user(id)
-----------------------------------------
shuup_companycontact: UNSHARDED
-----------------------------------------
shuup_companycontact_members: UNSHARDED
-----------------------------------------
shuup_shop: SHARDED
  owner_id             shards to auth_user            (explicit annotation)
      via   shuup_shop(owner_id) -> auth_user(id)
```

### 2. `auth_user` and `contact` are PII

Schema in `shuup/2-auth-user-and-contact-is-pii.sql`

```
-----------------------------------------
auth_user: DATASUBJECT
-----------------------------------------
shuup_contact: DATASUBJECT
-----------------------------------------
shuup_personcontact: SHARDED
  contact_ptr_id       shards to shuup_contact        (explicit annotation)
      via   shuup_personcontact(contact_ptr_id) -> shuup_contact(id)
  user_id              shards to auth_user            (explicit annotation)
      via   shuup_personcontact(user_id) -> auth_user(id)
-----------------------------------------
shuup_gdpr_gdpruserconsent: SHARDED
  user_id              shards to auth_user            (explicit annotation)
      via   shuup_gdpr_gdpruserconsent(user_id) -> auth_user(id)
-----------------------------------------
shuup_companycontact: SHARDED
  contact_ptr_id       shards to shuup_contact        (implicitly deduced)
      via   shuup_companycontact(contact_ptr_id) -> shuup_contact(id)
-----------------------------------------
shuup_companycontact_members: SHARDED
  contact_id           shards to shuup_contact        (explicit annotation)
      via   shuup_companycontact_members(contact_id) -> shuup_personcontact(pid)
      via   shuup_companycontact_members(contact_ptr_id) -> shuup_contact(id)
    with a total distance of 2
  contact_id           shards to auth_user            (explicit annotation)
      via   shuup_companycontact_members(contact_id) -> shuup_personcontact(pid)
      via   shuup_companycontact_members(user_id) -> auth_user(id)
    with a total distance of 2
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
shuup_shop: SHARDED
  owner_id             shards to auth_user            (explicit annotation)
      via   shuup_shop(owner_id) -> auth_user(id)
```

### 3. `contact` is PII

Schema in `shuup/3-contact-is-pii.sql`

```
-----------------------------------------
auth_user: SHARDED
  id                   shards to shuup_contact        (explicit annotation)
      via   auth_user(id) -> shuup_personcontact(user_id)
      via   auth_user(contact_ptr_id) -> shuup_contact(id)
    with a total distance of 2
  [Info]    This table is variably owned (via shuup_personcontact)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
shuup_contact: DATASUBJECT
-----------------------------------------
shuup_personcontact: SHARDED
  contact_ptr_id       shards to shuup_contact        (implicitly deduced)
      via   shuup_personcontact(contact_ptr_id) -> shuup_contact(id)
-----------------------------------------
shuup_gdpr_gdpruserconsent: SHARDED
  user_id              shards to shuup_contact        (explicit annotation)
      via   shuup_gdpr_gdpruserconsent(user_id) -> auth_user(id)
      via   shuup_gdpr_gdpruserconsent(id) -> shuup_personcontact(user_id)
      via   shuup_gdpr_gdpruserconsent(contact_ptr_id) -> shuup_contact(id)
    with a total distance of 3
  [Info]    This table is variably owned (via shuup_personcontact)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
shuup_companycontact: SHARDED
  contact_ptr_id       shards to shuup_contact        (explicit annotation)
      via   shuup_companycontact(contact_ptr_id) -> shuup_contact(id)
-----------------------------------------
shuup_companycontact_members: SHARDED
  companycontact_id    shards to shuup_contact        (implicitly deduced)
      via   shuup_companycontact_members(companycontact_id) -> shuup_companycontact(id)
      via   shuup_companycontact_members(contact_ptr_id) -> shuup_contact(id)
    with a total distance of 2
-----------------------------------------
shuup_shop: SHARDED
  owner_id             shards to shuup_contact        (explicit annotation)
      via   shuup_shop(owner_id) -> auth_user(id)
      via   shuup_shop(id) -> shuup_personcontact(user_id)
      via   shuup_shop(contact_ptr_id) -> shuup_contact(id)
    with a total distance of 3
  [Info]    This table is variably owned (via shuup_personcontact)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
```

### `shuup_personcontact` is PII

Schema in `shuup/4-person-contact-is-pii.sql`

```
-----------------------------------------
auth_user: SHARDED
  id                   shards to shuup_personcontact  (explicit annotation)
      via   auth_user(id) -> shuup_personcontact(user_id)
      via   auth_user(pid) -> shuup_personcontact(pid)
    with a total distance of 2
  [Info]    This table is variably owned (via shuup_personcontact)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
shuup_contact: SHARDED
  id                   shards to shuup_personcontact  (explicit annotation)
      via   shuup_contact(id) -> shuup_personcontact(contact_ptr_id)
      via   shuup_contact(pid) -> shuup_personcontact(pid)
    with a total distance of 2
  [Info]    This table is variably owned (via shuup_personcontact)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
shuup_personcontact: DATASUBJECT
-----------------------------------------
shuup_gdpr_gdpruserconsent: SHARDED
  user_id              shards to shuup_personcontact  (explicit annotation)
      via   shuup_gdpr_gdpruserconsent(user_id) -> auth_user(id)
      via   shuup_gdpr_gdpruserconsent(id) -> shuup_personcontact(user_id)
      via   shuup_gdpr_gdpruserconsent(pid) -> shuup_personcontact(pid)
    with a total distance of 3
  [Info]    This table is variably owned (via shuup_personcontact)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
shuup_companycontact: SHARDED
  contact_ptr_id       shards to shuup_personcontact  (implicitly deduced)
      via   shuup_companycontact(contact_ptr_id) -> shuup_contact(id)
      via   shuup_companycontact(id) -> shuup_personcontact(contact_ptr_id)
      via   shuup_companycontact(pid) -> shuup_personcontact(pid)
    with a total distance of 3
  [Info]    This table is variably owned (via shuup_personcontact)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
shuup_companycontact_members: SHARDED
  companycontact_id    shards to shuup_personcontact  (implicitly deduced)
      via   shuup_companycontact_members(companycontact_id) -> shuup_companycontact(contact_ptr_id)
      via   shuup_companycontact_members(contact_ptr_id) -> shuup_contact(id)
      via   shuup_companycontact_members(id) -> shuup_personcontact(contact_ptr_id)
      via   shuup_companycontact_members(pid) -> shuup_personcontact(pid)
    with a total distance of 4
  [Info]    This table is variably owned (via shuup_personcontact)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
shuup_shop: SHARDED
  owner_id             shards to shuup_personcontact  (explicit annotation)
      via   shuup_shop(owner_id) -> auth_user(id)
      via   shuup_shop(id) -> shuup_personcontact(user_id)
      via   shuup_shop(pid) -> shuup_personcontact(pid)
    with a total distance of 3
  [Info]    This table is variably owned (via shuup_personcontact)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
```

### `shuup_personcontact` is PII

Schema in `shuup/5-person-contact-is-pii-not-nullable.sql`

```
-----------------------------------------
auth_user: SHARDED
  id                   shards to shuup_personcontact  (explicit annotation)
      via   auth_user(id) -> shuup_personcontact(user_id)
      via   auth_user(pid) -> shuup_personcontact(pid)
    with a total distance of 2
  [Info]    This table is variably owned (via shuup_personcontact)
-----------------------------------------
shuup_contact: SHARDED
  id                   shards to shuup_personcontact  (explicit annotation)
      via   shuup_contact(id) -> shuup_personcontact(contact_ptr_id)
      via   shuup_contact(pid) -> shuup_personcontact(pid)
    with a total distance of 2
  [Info]    This table is variably owned (via shuup_personcontact)
-----------------------------------------
shuup_personcontact: DATASUBJECT
-----------------------------------------
shuup_gdpr_gdpruserconsent: SHARDED
  user_id              shards to shuup_personcontact  (explicit annotation)
      via   shuup_gdpr_gdpruserconsent(user_id) -> shuup_personcontact(pid)
-----------------------------------------
shuup_companycontact: SHARDED
  contact_ptr_id       shards to shuup_personcontact  (implicitly deduced)
      via   shuup_companycontact(contact_ptr_id) -> shuup_contact(id)
      via   shuup_companycontact(id) -> shuup_personcontact(contact_ptr_id)
      via   shuup_companycontact(pid) -> shuup_personcontact(pid)
    with a total distance of 3
  [Info]    This table is variably owned (via shuup_personcontact)
-----------------------------------------
shuup_companycontact_members: SHARDED
  companycontact_id    shards to shuup_personcontact  (implicitly deduced)
      via   shuup_companycontact_members(companycontact_id) -> shuup_companycontact(contact_ptr_id)
      via   shuup_companycontact_members(contact_ptr_id) -> shuup_contact(id)
      via   shuup_companycontact_members(id) -> shuup_personcontact(contact_ptr_id)
      via   shuup_companycontact_members(pid) -> shuup_personcontact(pid)
    with a total distance of 4
  [Info]    This table is variably owned (via shuup_personcontact)
-----------------------------------------
shuup_shop: SHARDED
  owner_id             shards to shuup_personcontact  (explicit annotation)
      via   shuup_shop(owner_id) -> shuup_personcontact(pid)
```

## ownCloud?

- `share_with_group` is `OWNER`
  - Makes `file` double var shared
  - Makes `share` var+N shared

Running `ownCloud.sql` produces

```
oc_share: sharded with
  OWNER_share_with_group shards to oc_users
    via oc_groups(gid) resolved with index users_for_group
    via oc_group_user(uid) resolved with index _unq_indx_1_uid
    total transitive distance is 2
  OWNER_uid_owner shards to oc_users
  [Info] this table is variably owned (via oc_group_user)

oc_group_user: sharded with
  uid shards to oc_users

oc_groups: sharded with
  gid shards to oc_users
    via oc_group_user(uid) resolved with index _unq_indx_1_uid
    total transitive distance is 1
  [Info] this table is variably owned (via oc_group_user)

oc_users: is PII

oc_files: sharded with
  id shards to oc_users
    via oc_share(OWNER_share_with_group) resolved with index users_for_file_via_group
    via oc_groups(gid) resolved with index users_for_group
    via oc_group_user(uid) resolved with index _unq_indx_1_uid
    total transitive distance is 3
  id shards to oc_users
    via oc_share(OWNER_uid_owner) resolved with index _unq_indx_2_OWNER_uid_owner
    total transitive distance is 1
  [SEVERE] This table is variably sharded 2 times in sequence. This will create oc_share*oc_group_user copies of records inserted into this table. This is likely not desired behavior, I suggest checking your `OWNS` annotations.
  [Warning] This table is variably owned in multiple ways (via oc_share and oc_share). This may not be desired behavior.
```
