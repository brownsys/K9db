#!/bin/bash
sudo service mariadb stop
sudo rm -rf /var/lib/mysql
sudo mkdir /var/lib/mysql
sudo chown mysql.mysql /var/lib/mysql
sudo chmod 0700 /var/lib/mysql
sudo mysql_install_db
sudo chown -R mysql.mysql /var/lib/mysql
sudo service mariadb start
mariadb -u root --execute="INSTALL SONAME 'ha_rocksdb';"
mariadb -u root --execute="SET PASSWORD FOR 'root'@'localhost' = PASSWORD('password');"
