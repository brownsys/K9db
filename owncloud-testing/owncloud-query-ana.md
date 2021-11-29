Parsing queries ...
Found 5167 log entries. 4847 queries parsed, 893 unique queries, 1 parse failures and 319 non-query entries.
## Overview

|Feature                                         | # Q|
|------------------------------------------------|----|
|[UPDATE WHERE [complex]](#update-where-complex) | 352|
|[SELECT WHERE [complex]](#select-where-complex) | 291|
|[IN [list]](#in-list)                           | 162|
|[INSERT](#insert)                               |  85|
|[ORDER BY](#order-by)                           |  84|
|[SELECT [point lookup]](#select-point-lookup)   |  80|
|[values](#values)                               |  75|
|[LIMIT](#limit)                                 |  58|
|[UPDATE [point lookup]](#update-point-lookup)   |  55|
|[join ON](#join-on)                             |  45|
|[function(GREATEST)](#function(greatest))       |  39|
|[INNER join](#inner-join)                       |  35|
|[DELETE [point lookup]](#delete-point-lookup)   |  21|
|[CASE](#case)                                   |  17|
|[operator `like`](#operator-like)               |  15|
|[DELETE WHERE [complex]](#delete-where-complex) |  14|
|[IN [subquery]](#in-subquery)                   |  13|
|[collate](#collate)                             |  12|
|[DISTINCT](#distinct)                           |  10|
|[LEFT OUTER join](#left-outer-join)             |  10|
|[HAVING](#having)                               |   9|
|[function(COUNT)](#function(count))             |   9|
|[GROUP BY](#group-by)                           |   8|
|[function(LOWER)](#function(lower))             |   6|
|[OFFSET](#offset)                               |   2|
|[SET](#set)                                     |   2|
|[SHOW VARIABLES](#show-variables)               |   1|
|[TRANSACTION](#transaction)                     |   1|

## Feature Detail
### `UPDATE WHERE [complex]`

Found in 352 unique queries.

- ``` UPDATE `oc_jobs` SET `reserved_at` = '1637957162', `last_checked` = '1637957162' WHERE (`id` = '14') AND (`reserved_at` = '0') AND (`last_checked` = '1637957101') ```
- ``` UPDATE `oc_jobs` SET `reserved_at` = '1637957162', `last_checked` = '1637957162' WHERE (`id` = '12') AND (`reserved_at` = '0') AND (`last_checked` = '1637957101') ```
- ``` UPDATE `oc_jobs` SET `reserved_at` = '1637957101', `last_checked` = '1637957101' WHERE (`id` = '7') AND (`reserved_at` = '0') AND (`last_checked` = '1637957041') ```
- ``` UPDATE `oc_jobs` SET `reserved_at` = '1637957281', `last_checked` = '1637957281' WHERE (`id` = '15') AND (`reserved_at` = '0') AND (`last_checked` = '1637957221') ```
- ``` UPDATE `oc_jobs` SET `reserved_at` = '1637957281', `last_checked` = '1637957281' WHERE (`id` = '4') AND (`reserved_at` = '0') AND (`last_checked` = '1637957221') ```
- ``` UPDATE `oc_appconfig` SET `configvalue` = '11' WHERE (`appid` = 'backgroundjob') AND (`configkey` = 'lastjob') AND (`configvalue` <> '11') ```
- ``` UPDATE `oc_filecache` SET `mimepart` = '3', `mimetype` = '5', `mtime` = '1637956902', `size` = '0', `etag` = '6bc446e0aad09761c1fa2aeb525b0fc1', `storage_mtime` = '1637956902', `permissions` = '27', `checksum` = '', `parent` = '8', `path_hash` = 'c89c560541b952a435783a7d51a27d50', `path` = 'files/Documents/Example.odt', `name` = 'Example.odt', `storage` = '1' WHERE (`storage` = '1') AND (`path_hash` = 'c89c560541b952a435783a7d51a27d50') ```
- ``` UPDATE `oc_filecache` SET `size` = '538942', `checksum` = 'SHA1:b26d5544e642330e2858a4d8528cb108ddbca9e3 MD5:147156bc95dba89fab2227507462ebea ADLER32:f21d8700' WHERE (`size` <> '538942' OR `checksum` <> 'SHA1:b26d5544e642330e2858a4d8528cb108ddbca9e3 MD5:147156bc95dba89fab2227507462ebea ADLER32:f21d8700' OR `size` IS NULL OR `checksum` IS NULL) AND `fileid` = '13' ```
- ``` UPDATE `oc_jobs` SET `reserved_at` = '1637956861', `last_checked` = '1637956861' WHERE (`id` = '14') AND (`reserved_at` = '0') AND (`last_checked` = '1637956801') ```
- ``` UPDATE `oc_jobs` SET `reserved_at` = '1637956861', `last_checked` = '1637956861' WHERE (`id` = '10') AND (`reserved_at` = '0') AND (`last_checked` = '1637956801') ```
### `SELECT WHERE [complex]`

Found in 291 unique queries.

- ``` SELECT * FROM `oc_jobs` WHERE `reserved_at` <= 1637913721 ORDER BY `last_checked` ASC LIMIT 1 ```
- ``` SELECT `object_id` AS `id`, COUNT(`object_id`) AS `count` FROM `oc_comments` AS c WHERE (`object_type` = 'files') AND (`object_id` IN ('10', '13', '11', '12')) AND (`object_id` NOT IN (SELECT `object_id` FROM `oc_comments_read_markers` AS crm WHERE (crm.`user_id` = 'admin') AND (crm.`marker_datetime` >= c.`creation_timestamp`) AND (c.`object_id` = crm.`object_id`))) GROUP BY `object_id` ```
- ``` SELECT SUM(`size`) AS f1, MIN(`size`) AS f2 FROM `oc_filecache` WHERE `parent` = '8' AND `storage` = '1' ```
- ``` SELECT `share_with`, `file_source`, `file_target` FROM `oc_share` WHERE `item_source` = '9' AND `share_type` = '1' AND `item_type` IN ('file', 'folder') ```
- ``` SELECT `fileid`, `storage`, `path`, `parent`, `name`, `mimetype`, `mimepart`, `size`, `mtime`, `storage_mtime`, `encrypted`, `etag`, `permissions`, `checksum` FROM `oc_filecache` WHERE `storage` = '1' AND `path_hash` = '6faab433a48974ab4ae1fb6910b507e5' ```
- ``` SELECT `principaluri`, `access` FROM `oc_dav_shares` WHERE (`resourceid` = '1') AND (`type` = 'addressbook') ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637956903' - `timeout`)) AND ((f.`path` = 'files/Documents') OR ((`depth` <> '0') AND (`path` IN ('files')))) ```
- ``` SELECT `fileid`, `storage`, `path`, `parent`, `name`, `mimetype`, `mimepart`, `size`, `mtime`, `storage_mtime`, `encrypted`, `etag`, `permissions`, `checksum` FROM `oc_filecache` WHERE `storage` = '1' AND `path_hash` = '0fea6a13c52b4d4725368f24b045ca84' ```
- ``` SELECT `id`, `uri`, `lastmodified`, `etag`, `size`, `carddata` FROM `oc_cards` WHERE (`addressbookid` = '1') AND (`uri` = 'Database:A user.vcf') LIMIT 1 ```
- ``` SELECT `id`, `uri`, `lastmodified`, `etag`, `calendarid`, `size`, `calendardata`, `componenttype`, `classification` FROM `oc_calendarobjects` WHERE (`calendarid` = '1') AND (`uri` = 'system-Database:A user.vcf-death.ics') ```
### `IN [list]`

Found in 162 unique queries.

- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637956950), `etag` = '61a13d561a7e9', `size` = CASE WHEN `size` > '-1' THEN `size` + '983' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '3b8779ba05b8f0aed49650f3ff8beb4b', '8f5aef7e96ff2fdf0521f79e40b29e7a')) ```
- ``` SELECT `share_with`, `file_source`, `file_target` FROM `oc_share` WHERE `item_source` = '11' AND `share_type` = '1' AND `item_type` IN ('file', 'folder') ```
- ``` SELECT * FROM `oc_share` WHERE (`share_type` = '6') AND (`file_source` IN (8)) AND ((`uid_owner` = 'admin') OR (`uid_initiator` = 'admin')) ORDER BY `id` ASC ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637956949' - `timeout`)) AND ((f.`path` = 'files/Photos') OR ((`depth` <> '0') AND (`path` IN ('files')))) ```
- ``` SELECT `id`, `uid`, `type`, `category` FROM `oc_vcategory` WHERE `uid` IN ('admin') AND `type` = 'files' ORDER BY `category` ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637956902), `etag` = '61a13d2677378' WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '45b963397aa40d4a0063e0d85e4fe7a1', 'd01bb67e7b71dd49fd06bad922f521c9')) ```
- ``` SELECT a.`id`, a.`uri`, a.`displayname`, a.`principaluri`, a.`description`, a.`synctoken`, s.`access` FROM `oc_dav_shares` AS s JOIN `oc_addressbooks` AS a ON s.`resourceid` = a.`id` WHERE (s.`principaluri` IN ('principals/groups/admin', 'principals/users/admin')) AND (a.`id` NOT IN (- 1)) AND (s.`type` = 'addressbook') ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637956948' - `timeout`)) AND ((f.`path` = 'files/Photos') OR ((`depth` <> '0') AND (`path` IN ('files')))) ```
- ``` SELECT `systemtagid`, `objectid` FROM `oc_systemtag_object_mapping` WHERE (`objectid` IN ('31')) AND (`objecttype` = 'files') ORDER BY `objectid` ASC, `systemtagid` ASC ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637956902), `etag` = '61a13d2669079' WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '45b963397aa40d4a0063e0d85e4fe7a1')) ```
### `INSERT`

Found in 85 unique queries.

- ``` INSERT INTO `oc_addressbooks` (`uri`, `displayname`, `description`, `principaluri`, `synctoken`) VALUES ('contacts', 'Contacts', NULL, 'principals/users/admin', '1') ```
- ``` INSERT INTO `oc_cards_properties` (`addressbookid`, `cardid`, `name`, `value`, `preferred`) VALUES ('1', '2', 'CLOUD', 'A user@localhost:8080', '0') ```
- ``` INSERT INTO `oc_activity` (`app`, `subject`, `subjectparams`, `message`, `messageparams`, `file`, `link`, `user`, `affecteduser`, `timestamp`, `priority`, `type`, `object_type`, `object_id`) VALUES ('files', 'changed_self', '[{\"31\":\"\\/Yet another file.txt\"}]', '', '[]', '/Yet another file.txt', 'http://localhost:8080/apps/files/?dir=/', 'admin', 'admin', '1637957140', '30', 'file_changed', 'files', '31') ```
- ``` INSERT INTO `oc_filecache` (`mimepart`, `mimetype`, `mtime`, `size`, `etag`, `storage_mtime`, `permissions`, `checksum`, `parent`, `path_hash`, `path`, `name`, `storage`) VALUES ('6', '7', '1637956902', '0', '8430723578aa1b7a3fd1c572c65a46ff', '1637956902', '27', '', '10', '99079102f2763830ef072aa7459c0b13', 'files/Photos/Portugal.jpg', 'Portugal.jpg', '1') ```
- ``` INSERT INTO `oc_cards_properties` (`addressbookid`, `cardid`, `name`, `value`, `preferred`) VALUES ('1', '2', 'N', 'user;A;;;', '0') ```
- ``` INSERT INTO `oc_mimetypes` (`mimetype`) VALUES ('image/jpeg') ```
- ``` INSERT INTO `oc_filecache` (`encrypted`, `mimepart`, `mimetype`, `mtime`, `size`, `etag`, `storage_mtime`, `permissions`, `checksum`, `parent`, `path_hash`, `path`, `name`, `storage`) VALUES ('0', '3', '13', '1637957075', '21', '30d9f3cf93a1a240f9d088ad710fdce8', '1637957075', '27', '', '25', '3c9b9f547e3a5727c9245844e4817ee3', 'files_versions/New text file.txt.v1637957074', 'New text file.txt.v1637957074', '1') ```
- ``` INSERT INTO `oc_filecache` (`mimepart`, `mimetype`, `mtime`, `size`, `etag`, `storage_mtime`, `permissions`, `checksum`, `parent`, `path_hash`, `path`, `name`, `storage`) VALUES ('6', '8', '1637956950', '1005', '0ae50f83cab64770574b8ec3fdce6383', '1637956950', '27', 'SHA1:7d611883a4a24584e5c63da94c2453cde6d4d5f9 MD5:af942b2c7436aa53908530273b3ddd45 ADLER32:b17b87f2', '21', '4ad3d81c7bfe7e9139f6802fbe1c4e63', 'thumbnails/13/32-32.png', '32-32.png', '1') ```
- ``` INSERT INTO `oc_mimetypes` (`mimetype`) VALUES ('text/plain') ```
- ``` INSERT INTO `oc_activity_mq` (`amq_appid`, `amq_subject`, `amq_subjectparams`, `amq_affecteduser`, `amq_timestamp`, `amq_type`, `amq_latest_send`) VALUES ('files_sharing', 'shared_with_by', '[{\"24\":\"\\/New text file.txt\"},\"admin\"]', 'A user', '1637957114', 'shared', '1637960714') ```
### `ORDER BY`

Found in 84 unique queries.

- ``` SELECT * FROM `oc_activity` WHERE (`affecteduser` = 'admin') AND (`type` IN ('file_created', 'file_changed', 'file_deleted', 'file_restored', 'shared', 'remote_share', 'public_links', 'comments', 'systemtags')) AND (`object_type` = 'files') AND (`object_id` = '31') ORDER BY `timestamp` DESC, `activity_id` DESC LIMIT 51 ```
- ``` SELECT `fileid`, `storage`, `path`, `parent`, `name`, `mimetype`, `mimepart`, `size`, `mtime`, `storage_mtime`, `encrypted`, `etag`, `permissions`, `checksum` FROM `oc_filecache` WHERE `parent` = 8 ORDER BY `name` ASC ```
- ``` SELECT `storage_id`, `root_id`, `user_id`, `mount_point` FROM `oc_mounts` WHERE `user_id` = 'admin' ORDER BY `storage_id` ASC ```
- ``` SELECT `systemtagid`, `objectid` FROM `oc_systemtag_object_mapping` WHERE (`objectid` IN ('24')) AND (`objecttype` = 'files') ORDER BY `objectid` ASC, `systemtagid` ASC ```
- ``` SELECT `fileid`, `storage`, `path`, `parent`, `name`, `mimetype`, `mimepart`, `size`, `mtime`, `storage_mtime`, `encrypted`, `etag`, `permissions`, `checksum` FROM `oc_filecache` WHERE `parent` = 15 ORDER BY `name` ASC ```
- ``` SELECT * FROM `oc_jobs` WHERE `reserved_at` <= 1637913962 ORDER BY `last_checked` ASC LIMIT 1 ```
- ``` SELECT * FROM `oc_jobs` WHERE `reserved_at` <= 1637913721 ORDER BY `last_checked` ASC LIMIT 1 ```
- ``` SELECT * FROM `oc_activity` WHERE (`affecteduser` = 'admin') AND (`type` IN ('file_created', 'file_changed', 'file_deleted', 'file_restored', 'shared', 'remote_share', 'public_links', 'comments', 'systemtags')) AND (`object_type` = 'files') AND (`object_id` = '24') ORDER BY `timestamp` DESC, `activity_id` DESC LIMIT 51 ```
- ``` SELECT * FROM `oc_share` WHERE ((`item_type` = 'file') OR (`item_type` = 'folder')) AND ((`share_type` = '0') OR (`share_type` = '1') OR (`share_type` = '3')) AND ((`uid_owner` = 'admin') OR (`uid_initiator` = 'admin')) AND (`file_source` IN (8)) ORDER BY `id` ASC ```
- ``` SELECT s.*, f.`fileid`, f.`path`, st.`id` AS `storage_string_id` FROM `oc_share` AS s LEFT JOIN `oc_filecache` AS f ON s.`file_source` = f.`fileid` LEFT JOIN `oc_storages` AS st ON f.`storage` = st.`numeric_id` WHERE (`share_type` = '0') AND (`share_with` = 'admin') AND ((`item_type` = 'file') OR (`item_type` = 'folder')) AND (`file_source` = '31') ORDER BY s.`id` ASC ```
### `SELECT [point lookup]`

Found in 80 unique queries.

- ``` SELECT * FROM `oc_share` WHERE `id` = '1' ```
- ``` SELECT `uid` FROM `oc_group_user` WHERE `gid` = 'My group' ORDER BY `uid` ASC LIMIT 50 OFFSET 50 ```
- ``` SELECT `gid` FROM `oc_groups` WHERE `gid` = 'My group' ```
- ``` SELECT `uid`, `password` FROM `oc_users` WHERE LOWER(`uid`) = LOWER('admin') ```
- ``` SELECT `uid` FROM `oc_group_user` WHERE `gid` = 'My group' ORDER BY `uid` ASC LIMIT 50 ```
- ``` SELECT `id` FROM `oc_mimetypes` WHERE `mimetype` = 'text' ```
- ``` INSERT INTO `oc_groups` (`gid`) SELECT 'My group' FROM `oc_groups` WHERE `gid` = 'My group' HAVING COUNT(*) = 0 ```
- ``` SELECT `appid`, `configkey`, `configvalue` FROM `oc_preferences` WHERE `userid` = 'A user' ```
- ``` SELECT `appid`, `configkey`, `configvalue` FROM `oc_preferences` WHERE `userid` = 'admin' ```
- ``` SELECT `id`, `uid`, `login_name`, `password`, `name`, `type`, `token`, `last_activity`, `last_check` FROM `oc_authtoken` WHERE `token` = 'fe44df1c4084f5fc61d31f1c2da0dbd934afc729e57ec9ef619f51926841d1024bde3c6fc561270512cdecc1932c7f2944c35775a518a9bf6e19e0c6b5d0336d' ```
### `values`

Found in 75 unique queries.

- ``` INSERT INTO `oc_share` (`share_type`, `share_with`, `accepted`, `item_type`, `item_source`, `file_source`, `permissions`, `attributes`, `uid_initiator`, `uid_owner`, `file_target`, `stime`) VALUES ('1', 'My group', 0, 'file', '31', '31', '19', NULL, 'admin', 'admin', '/Yet another file.txt', '1637957171') ```
- ``` INSERT INTO `oc_calendars` (`principaluri`, `uri`, `synctoken`, `transparent`, `components`, `displayname`, `calendarcolor`) VALUES ('principals/users/admin', 'personal', '1', '0', 'VEVENT,VTODO', 'Personal', '#041e42') ```
- ``` INSERT INTO `oc_activity_mq` (`amq_appid`, `amq_subject`, `amq_subjectparams`, `amq_affecteduser`, `amq_timestamp`, `amq_type`, `amq_latest_send`) VALUES ('files_sharing', 'shared_with_by', '[{\"24\":\"\\/New text file.txt\"},\"admin\"]', 'A user', '1637957114', 'shared', '1637960714') ```
- ``` INSERT INTO `oc_activity` (`app`, `subject`, `subjectparams`, `message`, `messageparams`, `file`, `link`, `user`, `affecteduser`, `timestamp`, `priority`, `type`, `object_type`, `object_id`) VALUES ('files_sharing', 'shared_group_self', '[{\"31\":\"\\/Yet another file.txt\"},\"My group\"]', '', '[]', '/Yet another file.txt', 'http://localhost:8080/apps/files/?dir=/', 'admin', 'admin', '1637957171', '30', 'shared', 'files', '31') ```
- ``` INSERT INTO `oc_mimetypes` (`mimetype`) VALUES ('text/plain') ```
- ``` INSERT INTO `oc_filecache` (`mimepart`, `mimetype`, `mtime`, `size`, `etag`, `storage_mtime`, `permissions`, `checksum`, `parent`, `path_hash`, `path`, `name`, `storage`) VALUES ('6', '8', '1637957140', '15319', '8d1962e4561c724b2d9c872d72e35f99', '1637957140', '27', 'SHA1:b2f85106eea5414cb84fe99d8121e921b9d9b88d MD5:2655f4b4ba072657d8c1552926384863 ADLER32:27db0026', '32', '1e960c45b53e9615ea8546ea1dcdb97a', 'thumbnails/31/2048-2048-max.png', '2048-2048-max.png', '1') ```
- ``` INSERT INTO `oc_filecache` (`mimepart`, `mimetype`, `mtime`, `size`, `etag`, `storage_mtime`, `permissions`, `checksum`, `path_hash`, `path`, `parent`, `name`, `storage`) VALUES ('1', '2', '1637956902', '-1', '61a13d2636842', '1637956902', '31', '', '0fea6a13c52b4d4725368f24b045ca84', 'cache', '4', 'cache', '1') ```
- ``` INSERT INTO `oc_filecache` (`mimepart`, `mimetype`, `mtime`, `size`, `etag`, `storage_mtime`, `permissions`, `checksum`, `parent`, `path_hash`, `path`, `name`, `storage`) VALUES ('6', '8', '1637957108', '1864', '85a6d797ed36f2a9646910c8950110d5', '1637957108', '27', 'SHA1:053eebcb6cf023421e81cab23087e4093b498266 MD5:2cff83d82e63667aefea2d3a32fb1b56 ADLER32:ef438d5b', '27', '48a4c49e479febc83c890bee43d30d5b', 'thumbnails/24/75-75.png', '75-75.png', '1') ```
- ``` INSERT INTO `oc_users` (`uid`, `password`) VALUES ('A user', '1|$2y$10$kD8/G9IMA.LcM5aJQ7HMPuDIrMw0aAi2yEKVP9HeSjfhoeS8NcZR.') ```
- ``` INSERT INTO `oc_filecache` (`mimepart`, `mimetype`, `mtime`, `size`, `etag`, `storage_mtime`, `permissions`, `checksum`, `parent`, `path_hash`, `path`, `name`, `storage`) VALUES ('11', '12', '1637957070', '0', '0a73e095729908bbdaaf5eccfc68ca22', '1637957070', '27', 'SHA1:da39a3ee5e6b4b0d3255bfef95601890afd80709 MD5:d41d8cd98f00b204e9800998ecf8427e ADLER32:00000001', '6', '139c1347bc5b209101fb5053fe8bd768', 'files/New text file.txt', 'New text file.txt', '1') ```
### `LIMIT`

Found in 58 unique queries.

- ``` SELECT * FROM `oc_jobs` WHERE `reserved_at` <= 1637913601 ORDER BY `last_checked` ASC LIMIT 1 ```
- ``` SELECT * FROM `oc_jobs` WHERE `reserved_at` <= 1637913901 ORDER BY `last_checked` ASC LIMIT 1 ```
- ``` SELECT `share_with` FROM `oc_share` WHERE `item_source` = '9' AND `share_type` = '6' AND `item_type` IN ('file', 'folder') LIMIT 1 ```
- ``` SELECT `gid` FROM `oc_groups` WHERE LOWER(`gid`) LIKE LOWER('%A user%') ORDER BY `gid` ASC LIMIT 200 ```
- ``` SELECT * FROM `oc_jobs` WHERE `reserved_at` <= 1637913721 ORDER BY `last_checked` ASC LIMIT 1 ```
- ``` SELECT `displayname`, `description`, `timezone`, `calendarorder`, `calendarcolor`, `id`, `uri`, `synctoken`, `components`, `principaluri`, `transparent` FROM `oc_calendars` WHERE (`uri` = 'contact_birthdays') AND (`principaluri` = 'principals/system/system') LIMIT 1 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%'))) AND (cp.`addressbookid` = '2')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT * FROM `oc_accounts` WHERE `user_id` COLLATE utf8mb4_general_ci LIKE '%%' ORDER BY `user_id` ASC LIMIT 500 OFFSET 500 ```
- ``` SELECT `share_with` FROM `oc_share` WHERE `item_source` = '8' AND `share_type` = '6' AND `item_type` IN ('file', 'folder') LIMIT 1 ```
- ``` SELECT `share_with` FROM `oc_share` WHERE `item_source` = '9' AND `share_type` = '3' AND `item_type` IN ('file', 'folder') LIMIT 1 ```
### `UPDATE [point lookup]`

Found in 55 unique queries.

- ``` UPDATE `oc_jobs` SET `reserved_at` = '0' WHERE `id` = 9 ```
- ``` UPDATE `oc_jobs` SET `reserved_at` = '0' WHERE `id` = 4 ```
- ``` UPDATE `oc_share` SET `uid_owner` = 'admin', `uid_initiator` = 'admin', `item_source` = '31', `file_source` = '31' WHERE `parent` = '2' ```
- ``` UPDATE `oc_jobs` SET `last_run` = 1637957041 WHERE `id` = 17 ```
- ``` UPDATE `oc_jobs` SET `execution_duration` = 0 WHERE `id` = 13 ```
- ``` UPDATE `oc_jobs` SET `reserved_at` = '0' WHERE `id` = 10 ```
- ``` UPDATE `oc_jobs` SET `last_run` = 1637957281 WHERE `id` = 17 ```
- ``` UPDATE `oc_authtoken` SET `last_check` = 1637957220, `last_activity` = 1637957220 WHERE `id` = 1 ```
- ``` UPDATE `oc_authtoken` SET `last_activity` = 1637956992 WHERE `id` = 1 ```
- ``` UPDATE `oc_jobs` SET `execution_duration` = 0 WHERE `id` = 12 ```
### `join ON`

Found in 45 unique queries.

- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637957160' - `timeout`)) AND (f.`path` = 'files') ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637957160' - `timeout`)) AND ((f.`path` = 'files/Documents') OR ((`depth` <> '0') AND (`path` IN ('files')))) ```
- ``` SELECT s.*, f.`fileid`, f.`path`, st.`id` AS `storage_string_id` FROM `oc_share` AS s LEFT JOIN `oc_filecache` AS f ON s.`file_source` = f.`fileid` LEFT JOIN `oc_storages` AS st ON f.`storage` = st.`numeric_id` WHERE (`share_type` = '0') AND (`share_with` = 'admin') AND ((`item_type` = 'file') OR (`item_type` = 'folder')) AND (`file_source` = '24') ORDER BY s.`id` ASC ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637956903' - `timeout`)) AND ((f.`path` = 'files/ownCloud Manual.pdf') OR ((`depth` <> '0') AND (`path` IN ('files')))) ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637956949' - `timeout`)) AND ((f.`path` = 'files/Photos') OR ((`depth` <> '0') AND (`path` IN ('files')))) ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637956951' - `timeout`)) AND (f.`path` = 'files') ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637957160' - `timeout`)) AND ((f.`path` = 'files/Yet another file.txt') OR ((`depth` <> '0') AND (`path` IN ('files')))) ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637957106' - `timeout`)) AND ((f.`path` = 'files/New text file.txt') OR ((`depth` <> '0') AND (`path` IN ('files')))) ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637957106' - `timeout`)) AND ((f.`path` = 'files/ownCloud Manual.pdf') OR ((`depth` <> '0') AND (`path` IN ('files')))) ```
- ``` SELECT DISTINCT a.`id` AS `id`, `user_id`, `lower_user_id`, `display_name`, `email`, `last_login`, `backend`, `state`, `quota`, `home` FROM `oc_accounts` AS a LEFT JOIN `oc_account_terms` AS t ON a.`id` = t.`account_id` WHERE (`lower_user_id` LIKE '%group%') OR (`display_name` COLLATE utf8mb4_general_ci LIKE '%group%') OR (`email` COLLATE utf8mb4_general_ci LIKE '%group%') OR (t.`term` LIKE '%group%') ORDER BY `display_name` ASC LIMIT 200 ```
### `function(GREATEST)`

Found in 39 unique queries.

- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637956902), `etag` = '61a13d263eb04' WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637956902), `etag` = '61a13d2639874' WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637956950), `etag` = '61a13d5623295' WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '3b8779ba05b8f0aed49650f3ff8beb4b')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637956950), `etag` = '61a13d5611a60', `size` = CASE WHEN `size` > '-1' THEN `size` + '-2' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637956902), `etag` = '61a13d266197d' WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '45b963397aa40d4a0063e0d85e4fe7a1', '0ad78ba05b6961d92f7970b2b3922eca')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637956950), `etag` = '61a13d5614877', `size` = CASE WHEN `size` > '-1' THEN `size` + '126124' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '3b8779ba05b8f0aed49650f3ff8beb4b', 'b8747019c45f55093926501ad0a5476e')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637957162), `etag` = '61a13e2ad9aa7', `size` = CASE WHEN `size` > '-1' THEN `size` + '1679' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '3b8779ba05b8f0aed49650f3ff8beb4b', '430f6d00632b5eaa2c1ab9a30bb1e4e5')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637957140), `etag` = '61a13e143c95e', `size` = CASE WHEN `size` > '-1' THEN `size` + '14' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '45b963397aa40d4a0063e0d85e4fe7a1')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637956950), `etag` = '61a13d5628b63', `size` = CASE WHEN `size` > '-1' THEN `size` + '1005' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '3b8779ba05b8f0aed49650f3ff8beb4b', '69db94a0be22d06c341db7a60516253a')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637956902), `etag` = '61a13d2677378' WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '45b963397aa40d4a0063e0d85e4fe7a1', 'd01bb67e7b71dd49fd06bad922f521c9')) ```
### `INNER join`

Found in 35 unique queries.

- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637956948' - `timeout`)) AND (f.`path` = 'files') ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637956948' - `timeout`)) AND ((f.`path` = 'files/ownCloud Manual.pdf') OR ((`depth` <> '0') AND (`path` IN ('files')))) ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637957134' - `timeout`)) AND ((f.`path` = 'files/Yet another file.txt') OR ((`depth` <> '0') AND (`path` IN ('files')))) ```
- ``` SELECT a.`id`, a.`uri`, a.`displayname`, a.`principaluri`, a.`description`, a.`synctoken`, s.`access` FROM `oc_dav_shares` AS s JOIN `oc_addressbooks` AS a ON s.`resourceid` = a.`id` WHERE (s.`principaluri` IN ('principals/system/system')) AND (a.`id` NOT IN (- 1, 1)) AND (s.`type` = 'addressbook') ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637957070' - `timeout`)) AND ((f.`path` = 'files/New text file.txt') OR ((`depth` <> '0') AND (`path` IN ('files')))) ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637956951' - `timeout`)) AND ((f.`path` = 'files/Photos') OR ((`depth` <> '0') AND (`path` IN ('files')))) ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637956951' - `timeout`)) AND (f.`path` = 'files') ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637956936' - `timeout`)) AND ((f.`path` = 'files/Documents/Example.odt') OR ((`depth` <> '0') AND (`path` IN ('files', 'files/Documents')))) ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637957106' - `timeout`)) AND ((f.`path` = 'files/New text file.txt') OR ((`depth` <> '0') AND (`path` IN ('files')))) ```
- ``` SELECT `id`, `owner`, `timeout`, `created_at`, `token`, `token`, `scope`, `depth`, `file_id`, `path`, `owner_account_id` FROM `oc_persistent_locks` AS l JOIN `oc_filecache` AS f ON l.`file_id` = f.`fileid` WHERE (`storage` = '1') AND (`created_at` > ('1637956948' - `timeout`)) AND ((f.`path` = 'files/Documents') OR ((`depth` <> '0') AND (`path` IN ('files')))) ```
### `DELETE [point lookup]`

Found in 21 unique queries.

- ``` DELETE FROM `oc_authtoken` WHERE `token` = '8b0e2ebaeb88385a7f40809be50c77822803cc2797129290022579f29bd0265ca677ff08736cfd57dd7479ddd47549bd09cfdc21d43ae01ffca041c014d52b6f' ```
- ``` DELETE FROM `oc_authtoken` WHERE `token` = '5b61e7d922dccc23d0830509c69473a2e419458ec8fa0f918079021e946d05a9477826c9fe60f379d68ed17cb2a996e41e3a5bcd0dce61614bed411f0065a4e2' ```
- ``` DELETE FROM `oc_authtoken` WHERE `token` = '950258e3b2d1a69cc6ebcf8dc55ceb683ffb009d6c88281f2166bcd8329934695f19023d01c1365116278e0e0fd99a001778a1391f29ab8114e107c64285c583' ```
- ``` DELETE FROM `oc_authtoken` WHERE `token` = '9501b0083008294ef193f8c1f743d4306d496735e4dbae66217110ba0cb3049051293b65e43f94dd0f65fd86ee38d3ad0c1bf12d2276b54b1f60cdd399ea95d1' ```
- ``` DELETE FROM `oc_authtoken` WHERE `token` = '537fb7694da12951a7a42ad802e0a0f4e6249ff4942ae1596d16b8083d56c41e8348e7618aef3926e73355dfa1be3d8bd84a99a97f4e4d56f8486685f51e9eb7' ```
- ``` DELETE FROM `oc_authtoken` WHERE `token` = '57d3f444b5b29aa57e54331cb89f4f93cffa67dc85aecca0ead5cb93b3df94d61b2fbb6c4f252a7ca5852aa04e421edbc454ff1a6bfe7903df828046655330c6' ```
- ``` DELETE FROM `oc_authtoken` WHERE `token` = '9f220934b73f2e183289dcd560fdbbad80ba41965abd96792615b6c035dab232a6f7d922b0cc3a52f84a15cd3d3dd415293ae109a1ae56d8988561be217144db' ```
- ``` DELETE FROM `oc_authtoken` WHERE `token` = '8702a1423e7d8da73f1d941b8e699f38a512b6621135b18f5986116cd3b29280e39f7585ad93706f1dcd2d2443fcf8de867fe39dc7e290e83ee1060df151201e' ```
- ``` DELETE FROM `oc_authtoken` WHERE `token` = '9d9924e0af9b2d8eda091404120ee6d763b7180a5244bf0fc0d295de91424ba61214621dd789839d7d3c3b4f17d48be7b147ea8ff291e36f260ef7ff762defc5' ```
- ``` DELETE FROM `oc_authtoken` WHERE `token` = 'dbdae592c8240df4c227ae40c31aa7a679ed421837855ec6daf55bc39a6fca5393c2d2b2d2be3546d54fb76c7b5b99f98c7a7de20eb2ce371484c838826960b4' ```
### `CASE`

Found in 17 unique queries.

- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637957162), `etag` = '61a13e2ad9aa7', `size` = CASE WHEN `size` > '-1' THEN `size` + '1679' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '3b8779ba05b8f0aed49650f3ff8beb4b', '430f6d00632b5eaa2c1ab9a30bb1e4e5')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637956950), `etag` = '61a13d56263d4', `size` = CASE WHEN `size` > '-1' THEN `size` + '210991' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '3b8779ba05b8f0aed49650f3ff8beb4b', '69db94a0be22d06c341db7a60516253a')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637957076), `etag` = '61a13dd498209', `size` = CASE WHEN `size` > '-1' THEN `size` + '676' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '3b8779ba05b8f0aed49650f3ff8beb4b', 'd1a8fc35c5ef899143f88432693f8263')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637957140), `etag` = '61a13e143c95e', `size` = CASE WHEN `size` > '-1' THEN `size` + '14' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '45b963397aa40d4a0063e0d85e4fe7a1')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637956950), `etag` = '61a13d5628b63', `size` = CASE WHEN `size` > '-1' THEN `size` + '1005' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '3b8779ba05b8f0aed49650f3ff8beb4b', '69db94a0be22d06c341db7a60516253a')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637957140), `etag` = '61a13e14b5b02', `size` = CASE WHEN `size` > '-1' THEN `size` + '638' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '3b8779ba05b8f0aed49650f3ff8beb4b', '430f6d00632b5eaa2c1ab9a30bb1e4e5')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637956950), `etag` = '61a13d56168f3', `size` = CASE WHEN `size` > '-1' THEN `size` + '987' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '3b8779ba05b8f0aed49650f3ff8beb4b', 'b8747019c45f55093926501ad0a5476e')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637957075), `etag` = '61a13dd30ba0f', `size` = CASE WHEN `size` > '-1' THEN `size` + '21' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '9692aae50022f45f1098646939b287b1')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637957074), `etag` = '61a13dd2e3108', `size` = CASE WHEN `size` > '-1' THEN `size` + '21' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '45b963397aa40d4a0063e0d85e4fe7a1')) ```
- ``` UPDATE `oc_filecache` SET `mtime` = GREATEST(`mtime`, 1637957076), `etag` = '61a13dd491958', `size` = CASE WHEN `size` > '-1' THEN `size` + '17753' ELSE `size` END WHERE (`storage` = 1) AND (`path_hash` IN ('d41d8cd98f00b204e9800998ecf8427e', '3b8779ba05b8f0aed49650f3ff8beb4b', 'd1a8fc35c5ef899143f88432693f8263')) ```
### `operator `like``

Found in 15 unique queries.

- ``` SELECT `gid` FROM `oc_groups` WHERE LOWER(`gid`) LIKE LOWER('%A group%') ORDER BY `gid` ASC LIMIT 200 ```
- ``` SELECT * FROM `oc_accounts` WHERE `user_id` COLLATE utf8mb4_general_ci LIKE '%%' ORDER BY `user_id` ASC LIMIT 500 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A user%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A user%'))) AND (cp.`addressbookid` = '2')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT DISTINCT a.`id` AS `id`, `user_id`, `lower_user_id`, `display_name`, `email`, `last_login`, `backend`, `state`, `quota`, `home` FROM `oc_accounts` AS a LEFT JOIN `oc_account_terms` AS t ON a.`id` = t.`account_id` WHERE (`lower_user_id` LIKE '%%') OR (`display_name` COLLATE utf8mb4_general_ci LIKE '%%') OR (`email` COLLATE utf8mb4_general_ci LIKE '%%') OR (t.`term` LIKE '%%') ORDER BY `display_name` ASC LIMIT 200 ```
- ``` SELECT `gid` FROM `oc_groups` WHERE LOWER(`gid`) LIKE LOWER('%A user%') ORDER BY `gid` ASC LIMIT 200 ```
- ``` SELECT `gid` FROM `oc_groups` WHERE LOWER(`gid`) LIKE LOWER('%group%') ORDER BY `gid` ASC LIMIT 200 ```
- ``` SELECT DISTINCT a.`id` AS `id`, `user_id`, `lower_user_id`, `display_name`, `email`, `last_login`, `backend`, `state`, `quota`, `home` FROM `oc_accounts` AS a LEFT JOIN `oc_account_terms` AS t ON a.`id` = t.`account_id` WHERE (`lower_user_id` LIKE '%a user%') OR (`display_name` COLLATE utf8mb4_general_ci LIKE '%A user%') OR (`email` COLLATE utf8mb4_general_ci LIKE '%A user%') OR (t.`term` LIKE '%a user%') ORDER BY `display_name` ASC LIMIT 200 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%'))) AND (cp.`addressbookid` = '2')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT DISTINCT a.`id` AS `id`, `user_id`, `lower_user_id`, `display_name`, `email`, `last_login`, `backend`, `state`, `quota`, `home` FROM `oc_accounts` AS a LEFT JOIN `oc_account_terms` AS t ON a.`id` = t.`account_id` WHERE (`lower_user_id` LIKE '%a group%') OR (`display_name` COLLATE utf8mb4_general_ci LIKE '%A group%') OR (`email` COLLATE utf8mb4_general_ci LIKE '%A group%') OR (t.`term` LIKE '%a group%') ORDER BY `display_name` ASC LIMIT 200 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%'))) AND (cp.`addressbookid` = '1')) ORDER BY c.`uri` ASC LIMIT 200 ```
### `DELETE WHERE [complex]`

Found in 14 unique queries.

- ``` DELETE FROM `oc_authtoken` WHERE (`last_activity` < 1637955721) AND (`type` = 0) ```
- ``` DELETE FROM `oc_authtoken` WHERE (`last_activity` < 1637955542) AND (`type` = 0) ```
- ``` DELETE FROM `oc_preferences` WHERE `userid` = 'admin' AND `appid` = 'owncloud' AND `configkey` = 'lostpassword' ```
- ``` DELETE FROM `oc_authtoken` WHERE (`last_activity` < 1637955661) AND (`type` = 0) ```
- ``` DELETE FROM `oc_authtoken` WHERE (`last_activity` < 1637956081) AND (`type` = 0) ```
- ``` DELETE FROM `oc_authtoken` WHERE (`last_activity` < 1637955601) AND (`type` = 0) ```
- ``` DELETE FROM `oc_authtoken` WHERE (`last_activity` < 1637955481) AND (`type` = 0) ```
- ``` DELETE FROM `oc_authtoken` WHERE (`last_activity` < 1637955962) AND (`type` = 0) ```
- ``` DELETE FROM `oc_authtoken` WHERE (`last_activity` < 1637956021) AND (`type` = 0) ```
- ``` DELETE FROM `oc_jobs` WHERE (`class` = 'OC\\Command\\CommandJob') AND (`argument` = '\"O:33:\\\"OCA\\\\Files_Versions\\\\Command\\\\Expire\\\":2:{s:43:\\\"\\u0000OCA\\\\Files_Versions\\\\Command\\\\Expire\\u0000fileName\\\";s:18:\\\"\\/New text file.txt\\\";s:39:\\\"\\u0000OCA\\\\Files_Versions\\\\Command\\\\Expire\\u0000user\\\";s:5:\\\"admin\\\";}\"') ```
### `IN [subquery]`

Found in 13 unique queries.

- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%'))) AND (cp.`addressbookid` = '2')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT `object_id` AS `id`, COUNT(`object_id`) AS `count` FROM `oc_comments` AS c WHERE (`object_type` = 'files') AND (`object_id` IN ('31')) AND (`object_id` NOT IN (SELECT `object_id` FROM `oc_comments_read_markers` AS crm WHERE (crm.`user_id` = 'admin') AND (crm.`marker_datetime` >= c.`creation_timestamp`) AND (c.`object_id` = crm.`object_id`))) GROUP BY `object_id` ```
- ``` SELECT `object_id` AS `id`, COUNT(`object_id`) AS `count` FROM `oc_comments` AS c WHERE (`object_type` = 'files') AND (`object_id` IN ('24')) AND (`object_id` NOT IN (SELECT `object_id` FROM `oc_comments_read_markers` AS crm WHERE (crm.`user_id` = 'admin') AND (crm.`marker_datetime` >= c.`creation_timestamp`) AND (c.`object_id` = crm.`object_id`))) GROUP BY `object_id` ```
- ``` SELECT `object_id` AS `id`, COUNT(`object_id`) AS `count` FROM `oc_comments` AS c WHERE (`object_type` = 'files') AND (`object_id` IN ('6', '8', '10', '7')) AND (`object_id` NOT IN (SELECT `object_id` FROM `oc_comments_read_markers` AS crm WHERE (crm.`user_id` = 'admin') AND (crm.`marker_datetime` >= c.`creation_timestamp`) AND (c.`object_id` = crm.`object_id`))) GROUP BY `object_id` ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A group%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A group%'))) AND (cp.`addressbookid` = '1')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT `object_id` AS `id`, COUNT(`object_id`) AS `count` FROM `oc_comments` AS c WHERE (`object_type` = 'files') AND (`object_id` IN ('8', '9')) AND (`object_id` NOT IN (SELECT `object_id` FROM `oc_comments_read_markers` AS crm WHERE (crm.`user_id` = 'admin') AND (crm.`marker_datetime` >= c.`creation_timestamp`) AND (c.`object_id` = crm.`object_id`))) GROUP BY `object_id` ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%'))) AND (cp.`addressbookid` = '1')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A user%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A user%'))) AND (cp.`addressbookid` = '2')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT `object_id` AS `id`, COUNT(`object_id`) AS `count` FROM `oc_comments` AS c WHERE (`object_type` = 'files') AND (`object_id` IN ('10', '13', '11', '12')) AND (`object_id` NOT IN (SELECT `object_id` FROM `oc_comments_read_markers` AS crm WHERE (crm.`user_id` = 'admin') AND (crm.`marker_datetime` >= c.`creation_timestamp`) AND (c.`object_id` = crm.`object_id`))) GROUP BY `object_id` ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A user%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A user%'))) AND (cp.`addressbookid` = '1')) ORDER BY c.`uri` ASC LIMIT 200 ```
### `collate`

Found in 12 unique queries.

- ``` SELECT DISTINCT a.`id` AS `id`, `user_id`, `lower_user_id`, `display_name`, `email`, `last_login`, `backend`, `state`, `quota`, `home` FROM `oc_accounts` AS a LEFT JOIN `oc_account_terms` AS t ON a.`id` = t.`account_id` WHERE (`lower_user_id` LIKE '%a user%') OR (`display_name` COLLATE utf8mb4_general_ci LIKE '%A user%') OR (`email` COLLATE utf8mb4_general_ci LIKE '%A user%') OR (t.`term` LIKE '%a user%') ORDER BY `display_name` ASC LIMIT 200 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A user%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A user%'))) AND (cp.`addressbookid` = '2')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%'))) AND (cp.`addressbookid` = '1')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A group%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A group%'))) AND (cp.`addressbookid` = '2')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A user%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A user%'))) AND (cp.`addressbookid` = '1')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT * FROM `oc_accounts` WHERE `user_id` COLLATE utf8mb4_general_ci LIKE '%%' ORDER BY `user_id` ASC LIMIT 500 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A group%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A group%'))) AND (cp.`addressbookid` = '1')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT DISTINCT a.`id` AS `id`, `user_id`, `lower_user_id`, `display_name`, `email`, `last_login`, `backend`, `state`, `quota`, `home` FROM `oc_accounts` AS a LEFT JOIN `oc_account_terms` AS t ON a.`id` = t.`account_id` WHERE (`lower_user_id` LIKE '%a group%') OR (`display_name` COLLATE utf8mb4_general_ci LIKE '%A group%') OR (`email` COLLATE utf8mb4_general_ci LIKE '%A group%') OR (t.`term` LIKE '%a group%') ORDER BY `display_name` ASC LIMIT 200 ```
- ``` SELECT DISTINCT a.`id` AS `id`, `user_id`, `lower_user_id`, `display_name`, `email`, `last_login`, `backend`, `state`, `quota`, `home` FROM `oc_accounts` AS a LEFT JOIN `oc_account_terms` AS t ON a.`id` = t.`account_id` WHERE (`lower_user_id` LIKE '%group%') OR (`display_name` COLLATE utf8mb4_general_ci LIKE '%group%') OR (`email` COLLATE utf8mb4_general_ci LIKE '%group%') OR (t.`term` LIKE '%group%') ORDER BY `display_name` ASC LIMIT 200 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%'))) AND (cp.`addressbookid` = '2')) ORDER BY c.`uri` ASC LIMIT 200 ```
### `DISTINCT`

Found in 10 unique queries.

- ``` SELECT DISTINCT a.`id` AS `id`, `user_id`, `lower_user_id`, `display_name`, `email`, `last_login`, `backend`, `state`, `quota`, `home` FROM `oc_accounts` AS a LEFT JOIN `oc_account_terms` AS t ON a.`id` = t.`account_id` WHERE (`lower_user_id` LIKE '%group%') OR (`display_name` COLLATE utf8mb4_general_ci LIKE '%group%') OR (`email` COLLATE utf8mb4_general_ci LIKE '%group%') OR (t.`term` LIKE '%group%') ORDER BY `display_name` ASC LIMIT 200 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A group%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A group%'))) AND (cp.`addressbookid` = '2')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%'))) AND (cp.`addressbookid` = '2')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT DISTINCT a.`id` AS `id`, `user_id`, `lower_user_id`, `display_name`, `email`, `last_login`, `backend`, `state`, `quota`, `home` FROM `oc_accounts` AS a LEFT JOIN `oc_account_terms` AS t ON a.`id` = t.`account_id` WHERE (`lower_user_id` LIKE '%a user%') OR (`display_name` COLLATE utf8mb4_general_ci LIKE '%A user%') OR (`email` COLLATE utf8mb4_general_ci LIKE '%A user%') OR (t.`term` LIKE '%a user%') ORDER BY `display_name` ASC LIMIT 200 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%group%'))) AND (cp.`addressbookid` = '1')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A user%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A user%'))) AND (cp.`addressbookid` = '2')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT DISTINCT a.`id` AS `id`, `user_id`, `lower_user_id`, `display_name`, `email`, `last_login`, `backend`, `state`, `quota`, `home` FROM `oc_accounts` AS a LEFT JOIN `oc_account_terms` AS t ON a.`id` = t.`account_id` WHERE (`lower_user_id` LIKE '%%') OR (`display_name` COLLATE utf8mb4_general_ci LIKE '%%') OR (`email` COLLATE utf8mb4_general_ci LIKE '%%') OR (t.`term` LIKE '%%') ORDER BY `display_name` ASC LIMIT 200 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A group%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A group%'))) AND (cp.`addressbookid` = '1')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT c.`carddata`, c.`uri` FROM `oc_cards` AS c WHERE c.`id` IN (SELECT DISTINCT cp.`cardid` FROM `oc_cards_properties` AS cp WHERE (((cp.`name` = 'CLOUD') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A user%')) OR ((cp.`name` = 'FN') AND (cp.`value` COLLATE utf8mb4_general_ci LIKE '%A user%'))) AND (cp.`addressbookid` = '1')) ORDER BY c.`uri` ASC LIMIT 200 ```
- ``` SELECT DISTINCT a.`id` AS `id`, `user_id`, `lower_user_id`, `display_name`, `email`, `last_login`, `backend`, `state`, `quota`, `home` FROM `oc_accounts` AS a LEFT JOIN `oc_account_terms` AS t ON a.`id` = t.`account_id` WHERE (`lower_user_id` LIKE '%a group%') OR (`display_name` COLLATE utf8mb4_general_ci LIKE '%A group%') OR (`email` COLLATE utf8mb4_general_ci LIKE '%A group%') OR (t.`term` LIKE '%a group%') ORDER BY `display_name` ASC LIMIT 200 ```
### `LEFT OUTER join`

Found in 10 unique queries.

- ``` SELECT s.*, f.`fileid`, f.`path`, st.`id` AS `storage_string_id` FROM `oc_share` AS s LEFT JOIN `oc_filecache` AS f ON s.`file_source` = f.`fileid` LEFT JOIN `oc_storages` AS st ON f.`storage` = st.`numeric_id` WHERE (`share_type` = '0') AND (`share_with` = 'admin') AND ((`item_type` = 'file') OR (`item_type` = 'folder')) AND (`file_source` = '24') ORDER BY s.`id` ASC ```
- ``` SELECT DISTINCT a.`id` AS `id`, `user_id`, `lower_user_id`, `display_name`, `email`, `last_login`, `backend`, `state`, `quota`, `home` FROM `oc_accounts` AS a LEFT JOIN `oc_account_terms` AS t ON a.`id` = t.`account_id` WHERE (`lower_user_id` LIKE '%a group%') OR (`display_name` COLLATE utf8mb4_general_ci LIKE '%A group%') OR (`email` COLLATE utf8mb4_general_ci LIKE '%A group%') OR (t.`term` LIKE '%a group%') ORDER BY `display_name` ASC LIMIT 200 ```
- ``` SELECT s.*, f.`fileid`, f.`path`, st.`id` AS `storage_string_id` FROM `oc_share` AS s LEFT JOIN `oc_filecache` AS f ON s.`file_source` = f.`fileid` LEFT JOIN `oc_storages` AS st ON f.`storage` = st.`numeric_id` WHERE (`share_type` = '0') AND (`share_with` = 'A user') AND ((`item_type` = 'file') OR (`item_type` = 'folder')) ORDER BY s.`id` ASC ```
- ``` SELECT DISTINCT a.`id` AS `id`, `user_id`, `lower_user_id`, `display_name`, `email`, `last_login`, `backend`, `state`, `quota`, `home` FROM `oc_accounts` AS a LEFT JOIN `oc_account_terms` AS t ON a.`id` = t.`account_id` WHERE (`lower_user_id` LIKE '%a user%') OR (`display_name` COLLATE utf8mb4_general_ci LIKE '%A user%') OR (`email` COLLATE utf8mb4_general_ci LIKE '%A user%') OR (t.`term` LIKE '%a user%') ORDER BY `display_name` ASC LIMIT 200 ```
- ``` SELECT DISTINCT a.`id` AS `id`, `user_id`, `lower_user_id`, `display_name`, `email`, `last_login`, `backend`, `state`, `quota`, `home` FROM `oc_accounts` AS a LEFT JOIN `oc_account_terms` AS t ON a.`id` = t.`account_id` WHERE (`lower_user_id` LIKE '%%') OR (`display_name` COLLATE utf8mb4_general_ci LIKE '%%') OR (`email` COLLATE utf8mb4_general_ci LIKE '%%') OR (t.`term` LIKE '%%') ORDER BY `display_name` ASC LIMIT 200 ```
- ``` SELECT DISTINCT a.`id` AS `id`, `user_id`, `lower_user_id`, `display_name`, `email`, `last_login`, `backend`, `state`, `quota`, `home` FROM `oc_accounts` AS a LEFT JOIN `oc_account_terms` AS t ON a.`id` = t.`account_id` WHERE (`lower_user_id` LIKE '%group%') OR (`display_name` COLLATE utf8mb4_general_ci LIKE '%group%') OR (`email` COLLATE utf8mb4_general_ci LIKE '%group%') OR (t.`term` LIKE '%group%') ORDER BY `display_name` ASC LIMIT 200 ```
- ``` SELECT s.*, f.`fileid`, f.`path`, st.`id` AS `storage_string_id` FROM `oc_share` AS s LEFT JOIN `oc_filecache` AS f ON s.`file_source` = f.`fileid` LEFT JOIN `oc_storages` AS st ON f.`storage` = st.`numeric_id` WHERE (`share_type` = '0') AND (`share_with` = 'admin') AND ((`item_type` = 'file') OR (`item_type` = 'folder')) AND (`file_source` = '31') ORDER BY s.`id` ASC ```
- ``` SELECT s.*, f.`fileid`, f.`path`, st.`id` AS `storage_string_id` FROM `oc_share` AS s LEFT JOIN `oc_filecache` AS f ON s.`file_source` = f.`fileid` LEFT JOIN `oc_storages` AS st ON f.`storage` = st.`numeric_id` WHERE (`file_source` = '31') AND (`share_type` = '1') AND (`share_with` IN ('admin')) AND ((`item_type` = 'file') OR (`item_type` = 'folder')) ORDER BY s.`id` ASC ```
- ``` SELECT s.*, f.`fileid`, f.`path`, st.`id` AS `storage_string_id` FROM `oc_share` AS s LEFT JOIN `oc_filecache` AS f ON s.`file_source` = f.`fileid` LEFT JOIN `oc_storages` AS st ON f.`storage` = st.`numeric_id` WHERE ((`share_type` = '1') AND (`share_with` IN ('admin'))) OR ((`share_type` = '0') AND (`share_with` = 'admin')) ORDER BY s.`id` ASC ```
- ``` SELECT s.*, f.`fileid`, f.`path`, st.`id` AS `storage_string_id` FROM `oc_share` AS s LEFT JOIN `oc_filecache` AS f ON s.`file_source` = f.`fileid` LEFT JOIN `oc_storages` AS st ON f.`storage` = st.`numeric_id` WHERE (`file_source` = '24') AND (`share_type` = '1') AND (`share_with` IN ('admin')) AND ((`item_type` = 'file') OR (`item_type` = 'folder')) ORDER BY s.`id` ASC ```
### `HAVING`

Found in 9 unique queries.

- ``` INSERT INTO `oc_appconfig` (`appid`, `configkey`, `configvalue`) SELECT 'core', 'umgmt_set_password', 'true' FROM `oc_appconfig` WHERE `appid` = 'core' AND `configkey` = 'umgmt_set_password' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_preferences` (`userid`, `appid`, `configkey`, `configvalue`) SELECT 'admin', 'firstrunwizard', 'show', '0' FROM `oc_preferences` WHERE `userid` = 'admin' AND `appid` = 'firstrunwizard' AND `configkey` = 'show' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_mounts` (`storage_id`, `root_id`, `user_id`, `mount_point`) SELECT '1', '4', 'admin', '/admin/' FROM `oc_mounts` WHERE `root_id` = '4' AND `user_id` = 'admin' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_preferences` (`userid`, `appid`, `configkey`, `configvalue`) SELECT 'admin', 'core', 'timezone', 'America/New_York' FROM `oc_preferences` WHERE `userid` = 'admin' AND `appid` = 'core' AND `configkey` = 'timezone' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_storages` (`id`, `available`) SELECT 'home::A user', '1' FROM `oc_storages` WHERE `id` = 'home::A user' AND `available` = '1' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_preferences` (`userid`, `appid`, `configkey`, `configvalue`) SELECT 'admin', 'files', 'file_sorting', 'name' FROM `oc_preferences` WHERE `userid` = 'admin' AND `appid` = 'files' AND `configkey` = 'file_sorting' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_groups` (`gid`) SELECT 'My group' FROM `oc_groups` WHERE `gid` = 'My group' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_preferences` (`userid`, `appid`, `configkey`, `configvalue`) SELECT 'admin', 'files', 'file_sorting_direction', 'asc' FROM `oc_preferences` WHERE `userid` = 'admin' AND `appid` = 'files' AND `configkey` = 'file_sorting_direction' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_preferences` (`userid`, `appid`, `configkey`, `configvalue`) SELECT 'admin', 'files', 'file_sorting_direction', 'desc' FROM `oc_preferences` WHERE `userid` = 'admin' AND `appid` = 'files' AND `configkey` = 'file_sorting_direction' HAVING COUNT(*) = 0 ```
### `function(COUNT)`

Found in 9 unique queries.

- ``` INSERT INTO `oc_preferences` (`userid`, `appid`, `configkey`, `configvalue`) SELECT 'admin', 'files', 'file_sorting', 'name' FROM `oc_preferences` WHERE `userid` = 'admin' AND `appid` = 'files' AND `configkey` = 'file_sorting' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_appconfig` (`appid`, `configkey`, `configvalue`) SELECT 'core', 'umgmt_set_password', 'true' FROM `oc_appconfig` WHERE `appid` = 'core' AND `configkey` = 'umgmt_set_password' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_storages` (`id`, `available`) SELECT 'home::A user', '1' FROM `oc_storages` WHERE `id` = 'home::A user' AND `available` = '1' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_groups` (`gid`) SELECT 'My group' FROM `oc_groups` WHERE `gid` = 'My group' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_mounts` (`storage_id`, `root_id`, `user_id`, `mount_point`) SELECT '1', '4', 'admin', '/admin/' FROM `oc_mounts` WHERE `root_id` = '4' AND `user_id` = 'admin' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_preferences` (`userid`, `appid`, `configkey`, `configvalue`) SELECT 'admin', 'firstrunwizard', 'show', '0' FROM `oc_preferences` WHERE `userid` = 'admin' AND `appid` = 'firstrunwizard' AND `configkey` = 'show' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_preferences` (`userid`, `appid`, `configkey`, `configvalue`) SELECT 'admin', 'files', 'file_sorting_direction', 'desc' FROM `oc_preferences` WHERE `userid` = 'admin' AND `appid` = 'files' AND `configkey` = 'file_sorting_direction' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_preferences` (`userid`, `appid`, `configkey`, `configvalue`) SELECT 'admin', 'core', 'timezone', 'America/New_York' FROM `oc_preferences` WHERE `userid` = 'admin' AND `appid` = 'core' AND `configkey` = 'timezone' HAVING COUNT(*) = 0 ```
- ``` INSERT INTO `oc_preferences` (`userid`, `appid`, `configkey`, `configvalue`) SELECT 'admin', 'files', 'file_sorting_direction', 'asc' FROM `oc_preferences` WHERE `userid` = 'admin' AND `appid` = 'files' AND `configkey` = 'file_sorting_direction' HAVING COUNT(*) = 0 ```
### `GROUP BY`

Found in 8 unique queries.

- ``` SELECT `object_id` AS `id`, COUNT(`object_id`) AS `count` FROM `oc_comments` AS c WHERE (`object_type` = 'files') AND (`object_id` IN ('6', '8', '24', '10', '7')) AND (`object_id` NOT IN (SELECT `object_id` FROM `oc_comments_read_markers` AS crm WHERE (crm.`user_id` = 'admin') AND (crm.`marker_datetime` >= c.`creation_timestamp`) AND (c.`object_id` = crm.`object_id`))) GROUP BY `object_id` ```
- ``` SELECT `object_id` AS `id`, COUNT(`object_id`) AS `count` FROM `oc_comments` AS c WHERE (`object_type` = 'files') AND (`object_id` IN ('31')) AND (`object_id` NOT IN (SELECT `object_id` FROM `oc_comments_read_markers` AS crm WHERE (crm.`user_id` = 'admin') AND (crm.`marker_datetime` >= c.`creation_timestamp`) AND (c.`object_id` = crm.`object_id`))) GROUP BY `object_id` ```
- ``` SELECT `object_id` AS `id`, COUNT(`object_id`) AS `count` FROM `oc_comments` AS c WHERE (`object_type` = 'files') AND (`object_id` IN ('8', '9')) AND (`object_id` NOT IN (SELECT `object_id` FROM `oc_comments_read_markers` AS crm WHERE (crm.`user_id` = 'admin') AND (crm.`marker_datetime` >= c.`creation_timestamp`) AND (c.`object_id` = crm.`object_id`))) GROUP BY `object_id` ```
- ``` SELECT `object_id` AS `id`, COUNT(`object_id`) AS `count` FROM `oc_comments` AS c WHERE (`object_type` = 'files') AND (`object_id` IN ('24')) AND (`object_id` NOT IN (SELECT `object_id` FROM `oc_comments_read_markers` AS crm WHERE (crm.`user_id` = 'admin') AND (crm.`marker_datetime` >= c.`creation_timestamp`) AND (c.`object_id` = crm.`object_id`))) GROUP BY `object_id` ```
- ``` SELECT `backend`, count(*) AS `count` FROM `oc_accounts` GROUP BY `backend` ```
- ``` SELECT `object_id` AS `id`, COUNT(`object_id`) AS `count` FROM `oc_comments` AS c WHERE (`object_type` = 'files') AND (`object_id` IN ('6', '8', '24', '10', '31', '7')) AND (`object_id` NOT IN (SELECT `object_id` FROM `oc_comments_read_markers` AS crm WHERE (crm.`user_id` = 'admin') AND (crm.`marker_datetime` >= c.`creation_timestamp`) AND (c.`object_id` = crm.`object_id`))) GROUP BY `object_id` ```
- ``` SELECT `object_id` AS `id`, COUNT(`object_id`) AS `count` FROM `oc_comments` AS c WHERE (`object_type` = 'files') AND (`object_id` IN ('10', '13', '11', '12')) AND (`object_id` NOT IN (SELECT `object_id` FROM `oc_comments_read_markers` AS crm WHERE (crm.`user_id` = 'admin') AND (crm.`marker_datetime` >= c.`creation_timestamp`) AND (c.`object_id` = crm.`object_id`))) GROUP BY `object_id` ```
- ``` SELECT `object_id` AS `id`, COUNT(`object_id`) AS `count` FROM `oc_comments` AS c WHERE (`object_type` = 'files') AND (`object_id` IN ('6', '8', '10', '7')) AND (`object_id` NOT IN (SELECT `object_id` FROM `oc_comments_read_markers` AS crm WHERE (crm.`user_id` = 'admin') AND (crm.`marker_datetime` >= c.`creation_timestamp`) AND (c.`object_id` = crm.`object_id`))) GROUP BY `object_id` ```
### `function(LOWER)`

Found in 6 unique queries.

- ``` SELECT `uid`, `password` FROM `oc_users` WHERE LOWER(`uid`) = LOWER('admin') ```
- ``` SELECT `gid` FROM `oc_groups` WHERE LOWER(`gid`) LIKE LOWER('%A user%') ORDER BY `gid` ASC LIMIT 200 ```
- ``` SELECT `gid` FROM `oc_groups` WHERE LOWER(`gid`) LIKE LOWER('%A group%') ORDER BY `gid` ASC LIMIT 200 ```
- ``` SELECT `uid`, `displayname` FROM `oc_users` WHERE LOWER(`uid`) = LOWER('A user') ```
- ``` SELECT `gid` FROM `oc_groups` WHERE LOWER(`gid`) LIKE LOWER('%group%') ORDER BY `gid` ASC LIMIT 200 ```
- ``` SELECT `uid`, `displayname` FROM `oc_users` WHERE LOWER(`uid`) = LOWER('admin') ```
### `OFFSET`

Found in 2 unique queries.

- ``` SELECT `uid` FROM `oc_group_user` WHERE `gid` = 'My group' ORDER BY `uid` ASC LIMIT 50 OFFSET 50 ```
- ``` SELECT * FROM `oc_accounts` WHERE `user_id` COLLATE utf8mb4_general_ci LIKE '%%' ORDER BY `user_id` ASC LIMIT 500 OFFSET 500 ```
### `SET`

Found in 2 unique queries.

- ``` SET AUTOCOMMIT = 1 ```
- ``` SET dummy = NULL ```
### `SHOW VARIABLES`

Found in 1 unique queries.

- ``` SHOW variables like ```
### `TRANSACTION`

Found in 1 unique queries.

- ``` START TRANSACTION ```
