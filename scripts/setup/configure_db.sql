CREATE USER 'k9db'@'%' IDENTIFIED BY 'password'
\p;
GRANT ALL PRIVILEGES ON *.* TO 'k9db'@'%' IDENTIFIED BY 'password'
\p;
FLUSH PRIVILEGES
\p;
INSTALL SONAME 'ha_rocksdb'
\p;
