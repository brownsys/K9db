#!/bin/bash

# Our current location is the root dir of the repo.
K9DB_DIR="$(pwd)"

# remove any installed mysql server.
echo "Removing any existing mysql server..."
sudo apt-get remove -y --purge mysql* mariadb*

# install mariadb.
echo "Installing MariaDB..."
cd /tmp
sudo curl -LsS -O https://downloads.mariadb.com/MariaDB/mariadb_repo_setup
sudo bash mariadb_repo_setup --mariadb-server-version=10.6
sudo rm mariadb_repo_setup
sudo apt-get install -y mariadb-server-10.6 mariadb-client-10.6 mariadb-plugin-rocksdb

# Start mariadb, configure mariadb user.
echo "Starting MariaDB and configuring user..."
cd $K9DB_DIR
sudo service mariadb start
sudo mariadb -u root < configure_db.sql

# Configure mariadb for optimized performance.
sudo service mariadb stop
echo "Configuring MariaDB performance...."
echo "[mysqld]" | sudo tee -a /etc/mysql/mariadb.cnf
echo "table_open_cache_instances = 1" | sudo tee -a /etc/mysql/mariadb.cnf
echo "table_open_cache = 1000000" | sudo tee -a /etc/mysql/mariadb.cnf
echo "table_definition_cache = 1000000" | sudo tee -a /etc/mysql/mariadb.cnf
echo "open_files_limit = 1000000" | sudo tee -a /etc/mysql/mariadb.cnf

# ulimits.
echo "* hard nofile 1024000" | sudo tee -a /etc/security/limits.conf
echo "* soft nofile 1024000" | sudo tee -a /etc/security/limits.conf
echo "mysql hard nofile 1024000" | sudo tee -a /etc/security/limits.conf
echo "mysql soft nofile 1024000" | sudo tee -a /etc/security/limits.conf
echo "root hard nofile 1024000" | sudo tee -a /etc/security/limits.conf
echo "root soft nofile 1024000" | sudo tee -a /etc/security/limits.conf
echo "* soft nofile 1024000" | sudo tee -a /etc/security/limits.d/90-nproc.conf
echo "* hard nofile 1024000" | sudo tee -a /etc/security/limits.d/90-nproc.conf
echo "* soft nproc 10240" | sudo tee -a /etc/security/limits.d/90-nproc.conf
echo "* hard nproc 10240" | sudo tee -a /etc/security/limits.d/90-nproc.conf
echo "root soft nproc unlimited" | sudo tee -a /etc/security/limits.d/90-nproc.conf

# Install mariadb C++ connector.
echo "Installing MariaDB c++ connector..."
cd /tmp
sudo apt-get install -y libmariadb3 libmariadb-dev
sudo wget https://dlm.mariadb.com/1601342/Connectors/cpp/connector-cpp-1.0.0/mariadb-connector-cpp-1.0.0-ubuntu-groovy-amd64.tar.gz
sudo tar -xvzf mariadb-connector-cpp-1.0.0-*.tar.gz
sudo rm mariadb-connector-cpp-1.0.0-*.tar.gz
cd mariadb-connector-cpp-1.0.0-*
sudo install -d /usr/include/mariadb/conncpp
sudo install -d /usr/include/mariadb/conncpp/compat
sudo install -v include/mariadb/*.hpp /usr/include/mariadb/
sudo install -v include/mariadb/conncpp/*.hpp /usr/include/mariadb/conncpp
sudo install -v include/mariadb/conncpp/compat/* /usr/include/mariadb/conncpp/compat
sudo install -d /usr/lib/mariadb
sudo install -d /usr/lib/mariadb/plugin
sudo install -v lib64/mariadb/libmariadbcpp.so /usr/lib
sudo install -v lib64/mariadb/plugin/* /usr/lib/mariadb/plugin
cd /tmp
sudo rm -rf mariadb*

# Configure MariaDB to use SSD.
echo "Configuring MariaDB SSD..."
cd $K9DB_DIR
./experiments/scripts/setup/ssd.sh
echo 'datadir = /mnt/disks/my-ssd/mysql' | sudo tee -a /etc/mysql/mariadb.cnf

# Configure MariaDB to listen on the external network interface.
echo "Configuring MariaDB network interface..."
LOCAL_IP=$(curl  http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip -H "Metadata-Flavor: Google")
echo "LOCAL_IP=\"$LOCAL_IP\"" >> ~/.bashrc
sudo sed -i "s|bind-address.*|bind-address = $LOCAL_IP|" /etc/mysql/mariadb.conf.d/50-server.cnf
