#!/bin/bash

# Format and attach SSD.
mkfs.ext4 -F /dev/nvme0n1
mkdir -p /mnt/disks/my-ssd
mount /dev/nvme0n1 /mnt/disks/my-ssd
chown mysql.mysql /mnt/disks/my-ssd
chmod 777 /mnt/disks/my-ssd
sudo cp /var/lib/mysql /mnt/disks/my-ssd/ -r
chown mysql.mysql /mnt/disks/my-ssd/mysql


# Configure mariadb to store database in directory
echo "[mysqld]" >> /etc/mysql/mariadb.cnf
echo "datadir=/mnt/disks/my-ssd/mysql" >> /etc/mysql/mariadb.cnf
echo "socket=/mnt/disks/my-ssd/mysql.sock" >> /etc/mysql/mariadb.cnf
