CREATE USER 'usr'@'%' IDENTIFIED BY 'password'
\p;
GRANT ALL PRIVILEGES ON *.* TO 'usr'@'%' IDENTIFIED BY 'password'
\p;
FLUSH PRIVILEGES
\p;
INSTALL SONAME 'ha_rocksdb'
\p;
