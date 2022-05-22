CREATE VIEW shuup_contact_owned_by_shuup_personcontact AS '"SELECT OWNING_contact_ptr_id, pid, COUNT(*) FROM shuup_personcontact GROUP BY OWNING_contact_ptr_id, pid HAVING OWNING_contact_ptr_id = ?"';
CREATE VIEW auth_user_owned_by_shuup_personcontact AS '"SELECT OWNING_user_id, pid, COUNT(*) FROM shuup_personcontact GROUP BY OWNING_user_id, pid HAVING OWNING_user_id = ?"';

CREATE VIEW auth_personcontact AS '"SELECT auth_user.id, shuup_personcontact.pid, COUNT(*) FROM auth_user JOIN shuup_personcontact ON auth_user.ptr = shuup_personcontact.OWNING_user_id WHERE auth_user.id = ? GROUP BY (auth_user.id, shuup_personcontact.pid) HAVING COUNT(*) > 0"';

CREATE VIEW contact_personcontact AS '"SELECT shuup_contact.id, shuup_personcontact.pid, COUNT(*) FROM shuup_contact JOIN shuup_personcontact ON shuup_contact.ptr = shuup_personcontact.OWNING_contact_ptr_id WHERE shuup_contact.id = ? GROUP BY (shuup_contact.id, shuup_personcontact.pid) HAVING COUNT(*) > 0"';

CREATE VIEW gdpr_to_personcontact AS '"SELECT shuup_shop.id, shuup_personcontact.pid, COUNT(*) FROM auth_user JOIN shuup_personcontact ON auth_user.ptr = shuup_personcontact.OWNING_user_id JOIN shuup_shop ON auth_user.id = shuup_shop.OWNER_owner_id WHERE shuup_shop.id = ? GROUP BY (shuup_shop.id, shuup_personcontact.pid) HAVING COUNT(*) > 0"';