USE mysql;
UPDATE user SET plugin='' WHERE User='root';
SET PASSWORD = PASSWORD('password');
flush privileges;
INSTALL SONAME 'ha_rocksdb';
