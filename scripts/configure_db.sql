CREATE USER 'pelton'@'%' IDENTIFIED BY 'password'
\p;
GRANT ALL PRIVILEGES ON *.* TO 'pelton'@'%' IDENTIFIED BY 'password'
\p;
FLUSH PRIVILEGES
\p;
INSTALL SONAME 'ha_rocksdb'
\p;
