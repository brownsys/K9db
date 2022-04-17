# EXPLAIN Scenarios

## Shuup schema

Base schema in `shuup/base.sql`

- Who’s the PII ? (Shuup contact, auth_user or personcontact)
    - `auth_user` is PII: guest users are `personcontact`s ⇒ they have addresses etc. but they would not be sharded, because `personcontact.user_id` column is `NULL`
        
        Explain should tell you that there may be no owner in some cases
        
    - `auth_user` and `contact` is PII (because explain flagged the nullable `personcontact.user_id`) ⇒ `OWNS` into PII creates warning
    - `contact` is PII: overcompliant but thats fine. The developer will figure this out by looking at how things are sharded
- Forgot `OWNS` on `contact_ptr_id` ⇒ Warning that `contact` has `email` etc fields but is unowned
- If no `OWNS` to `contact` then flag that there are `email` etc type columns
- Three `OWNER` annotations on `basket`

### No PII

Schema in `shuup/no-pii.sql`

```
shuup_shop: unsharded

shuup_companycontact_members: unsharded

shuup_companycontact: unsharded

shuup_gdpr_gdpruserconsent: unsharded

shuup_personcontact: unsharded
  [Warning] The column "email" sounds like it may belong to a data subject, but the table is not sharded. You may want to review your annotations.

shuup_contact: unsharded

auth_user: unsharded
  [Warning] The columns "username", "password" sound like they may belong to a data subject, but the table is not sharded. You may want to review your annotations.
```

### `auth_user` is PII

Schema in `shuup/auth-user-is-pii.sql`

```
shuup_shop: sharded with
  OWNER_owner_id shards to auth_user

shuup_companycontact_members: unsharded

shuup_companycontact: unsharded

shuup_gdpr_gdpruserconsent: sharded with
  OWNER_user_id shards to auth_user

shuup_personcontact: sharded with
  user_id shards to auth_user
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.

shuup_contact: unsharded
  [Warning] The column "email" sounds like it may belong to a data subject, but the table is not sharded. You may want to review your annotations.

auth_user: is PII
```

### `contact` is PII

Schema in `shuup/contact-is-pii.sql`

```
shuup_shop: sharded with
  OWNER_owner_id shards to shuup_contact
    via auth_user(ptr) resolved with index auth_user_ptr_ptr
    total transitive distance is 2
  [Info] this table is variably owned (via shuup_personcontact)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.

shuup_companycontact_members: sharded with
  companycontact_id shards to shuup_contact
    via shuup_companycontact(contact_ptr_id) resolved with index company_contact_ptr_id_contact_ptr_id
    total transitive distance is 1

shuup_companycontact: sharded with
  contact_ptr_id shards to shuup_contact

shuup_gdpr_gdpruserconsent: sharded with
  OWNER_user_id shards to shuup_contact
    via auth_user(ptr) resolved with index auth_user_ptr_ptr
    total transitive distance is 2
  [Info] this table is variably owned (via shuup_personcontact)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.

shuup_personcontact: sharded with
  contact_ptr_id shards to shuup_contact

shuup_contact: is PII

auth_user: sharded with
  ptr shards to shuup_contact
    total transitive distance is 1
  [Info] this table is variably owned (via shuup_personcontact)
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.
```

### `auth_user` and `contact` are PII

Schema in `auth-user-and-contact-is-pii.sql`

```
shuup_shop: sharded with
  OWNER_owner_id shards to auth_user

shuup_companycontact_members: sharded with
  OWNER_contact_id shards to shuup_contact
    total transitive distance is 1
  OWNER_contact_id shards to auth_user
    total transitive distance is 1
  [Warning] This table is sharded, but all sharding paths are nullable. This will lead to records being stored in a global table if those columns are NULL and could be a source of non-compliance.

shuup_companycontact: sharded with
  contact_ptr_id shards to shuup_contact

shuup_gdpr_gdpruserconsent: sharded with
  OWNER_user_id shards to auth_user

shuup_personcontact: sharded with
  OWNER_contact_ptr_id shards to shuup_contact
  OWNER_user_id shards to auth_user

shuup_contact: is PII

auth_user: is PII
```

After adding a `OWNS` to `personcontact(contact_ptr_id)` we get the error

```
INVALID_ARGUMENT: Foreign key shuup_companycontact(contact_ptr_id) points to shuup_contact which is a PII and sharded table
```

when creating `companycontact`. 

It’s a start but we probably want this to fail earlier (e.g. either when installing `personcontact`)

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