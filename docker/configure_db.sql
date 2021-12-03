ALTER USER 'root'@'localhost' IDENTIFIED BY 'password'
\p;
FLUSH PRIVILEGES
\p;
INSTALL SONAME 'ha_rocksdb'
\p;
