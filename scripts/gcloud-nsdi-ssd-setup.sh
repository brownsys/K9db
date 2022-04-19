#!/bin/bash

# Format and attach SSD.
mkfs.ext4 -F /dev/nvme0n1
mkdir -p /mnt/disks/my-ssd
mount /dev/nvme0n1 /mnt/disks/my-ssd
chown mariadb /mnt/disks/my-ssd

# Configure mariadb to store database in directory
echo "[mysqld]" >> /etc/mysql/mariadb.cnf
echo "datadir=/mnt/disks/my-ssd" >> /etc/mysql/mariadb.cnf
echo "socket=/mnt/disks/my-ssd/mysql.sock" >> /etc/mysql/mariadb.cnf
