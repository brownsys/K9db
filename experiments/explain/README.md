# EXPLAIN Scenarios

This experiment provides a step-by-step example of how to annotate the schema of an e-commerce platform called Shuup to achieve compliance with privacy laws like Europe's General Data Protection Regulation (GDPR).

## Quickstart

1. Start k9db.

```
   bazel run :k9db -- --logtostderr=1
```

2. Run the schema file via the mysql proxy to see output produced by K9db's `EXPLAIN COMPLIANCE` command.

```
mariadb P100001 < experiments/explain/shuup/{schema_name}.sql
```

## Step-by-step explanation

Looking at `shuup/0-no-pii.sql`, which table is the data subject (i.e. the table containing Personally Identifiable Information, PII): `shuup_contact`, `auth_user` or `shuup_personcontact`?

Shuup’s schema has several tables that might correspond to data subjects. K9db’s `EXPLAIN COMPLIANCE` helps the developer understand that they need to annotate `personcontact`.

0. First, let's run `EXPLAIN` on our base, unannotated schema to receive guidance on which tables might correspond to data subjects. K9db will flag several columns which sound like they could correspond to data subjects in the tables `auth_user`, `shuup_contact`, and `shuup_personcontact`.

1. `auth_user` is PII. A developer's first step might be to annotate `auth_user`, the login details table, as containing PII - this is plausible, but incompliant. It results in `shuup_contact` being unconnected to the ownership graph, as there are no foreign keys `auth_user`. The `personcontact` table has such a foreign key (`personcontact.user_id`), but it is nullable (e.g., for guest customers who lack accounts), and thus some of its rows will be stored in μDBs and others in the orphaned region. Explain also warns us that `shuup_contact` has `email` etc fields but is unowned, indicating that the developer should add an `OWNS` annotation to `contact_ptr_id`.

2. `auth_user` and `contact` are PII. Annotating both tables appears plausible, but is overcompliant - see 3. below for an explanation.

3. `contact` is PII. A developer might also annotate `contact` with `DATA SUBJECT`, but that table includes entries for customers and companies. Annotating it makes companies into data subjects, will duplicate various company-related tables across μDBs. `EXPLAIN COMPLIANCE` alerts the developer to this.

4. `shuup_personcontact` is PII. We annotate personcontact with `DATA SUBJECT`. This table stores natural persons, and has FKs to their contact information (in `contact`) and their logins (in `auth_user`) if they have accounts. Thus, `personcontact` connects users with and users without accounts.

## Explain Compliance output

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

### 4. `shuup_personcontact` is PII

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

### 5. `shuup_personcontact` is PII

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

## ownCloud

Running the annotated `ownCloud.sql` with K9db's `EXPLAIN COMPLIANCE` command produces the following output.

```
-----------------------------------------
oc_storages: UNSHARDED
-----------------------------------------
oc_users: DATASUBJECT
-----------------------------------------
oc_groups: SHARDED
  gid                  shards to oc_users             (explicit annotation)
      via   oc_groups(gid) -> oc_group_user(gid)
      via   oc_groups(uid) -> oc_users(uid)
    with a total distance of 2
  [Info]    This table is variably owned (via oc_group_user)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
oc_group_user: SHARDED
  uid                  shards to oc_users             (explicit annotation)
      via   oc_group_user(uid) -> oc_users(uid)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
oc_files: SHARDED
  id                   shards to oc_users             (explicit annotation)
      via   oc_files(id) -> oc_share(item_source)
      via   oc_files(share_with) -> oc_users(uid)
    with a total distance of 2
  id                   shards to oc_users             (explicit annotation)
      via   oc_files(id) -> oc_share(item_source)
      via   oc_files(share_with_group) -> oc_groups(gid)
      via   oc_files(gid) -> oc_group_user(gid)
      via   oc_files(uid) -> oc_users(uid)
    with a total distance of 4
  id                   shards to oc_users             (explicit annotation)
      via   oc_files(id) -> oc_share(item_source)
      via   oc_files(uid_owner) -> oc_users(uid)
    with a total distance of 2
  [SEVERE]  This table is variably sharded 2 times in sequence. This will create |oc_share|*|oc_group_user| copies of records inserted into this table. This is likely not desired behavior, I suggest checking your `OWNS` annotations.
  [Warning] This table is variably owned in multiple ways (via oc_share and oc_share and oc_share). This may not be desired behavior.
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
oc_share: SHARDED
  share_with           shards to oc_users             (explicit annotation)
      via   oc_share(share_with) -> oc_users(uid)
  share_with_group     shards to oc_users             (explicit annotation)
      via   oc_share(share_with_group) -> oc_groups(gid)
      via   oc_share(gid) -> oc_group_user(gid)
      via   oc_share(uid) -> oc_users(uid)
    with a total distance of 3
  uid_owner            shards to oc_users             (explicit annotation)
      via   oc_share(uid_owner) -> oc_users(uid)
  [Info]    This table is variably owned (via oc_group_user)
  [Warning] This table is variably owned and also copied an additional 2 times.
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
-----------------------------------------
oc_filecache: SHARDED
  fileid               shards to oc_users             (explicit annotation)
      via   oc_filecache(fileid) -> oc_files(id)
      via   oc_filecache(id) -> oc_share(item_source)
      via   oc_filecache(share_with) -> oc_users(uid)
    with a total distance of 3
  fileid               shards to oc_users             (explicit annotation)
      via   oc_filecache(fileid) -> oc_files(id)
      via   oc_filecache(id) -> oc_share(item_source)
      via   oc_filecache(share_with_group) -> oc_groups(gid)
      via   oc_filecache(gid) -> oc_group_user(gid)
      via   oc_filecache(uid) -> oc_users(uid)
    with a total distance of 5
  fileid               shards to oc_users             (explicit annotation)
      via   oc_filecache(fileid) -> oc_files(id)
      via   oc_filecache(id) -> oc_share(item_source)
      via   oc_filecache(uid_owner) -> oc_users(uid)
    with a total distance of 3
  [SEVERE]  This table is variably sharded 2 times in sequence. This will create |oc_share|*|oc_group_user| copies of records inserted into this table. This is likely not desired behavior, I suggest checking your `OWNS` annotations.
  [Warning] This table is variably owned in multiple ways (via oc_share and oc_share and oc_share). This may not be desired behavior.
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
```
