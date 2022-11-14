#!/bin/bash
service mariadb stop
umount /mnt/disks/my-ssd || /bin/true

# Format and attach SSD.
mkfs.ext4 -F /dev/nvme0n1
mkdir -p /mnt/disks/my-ssd
mount /dev/nvme0n1 /mnt/disks/my-ssd
chown mysql.mysql /mnt/disks/my-ssd
cp /var/lib/mysql /mnt/disks/my-ssd/ -r
chown mysql.mysql -R /mnt/disks/my-ssd/mysql
mkdir -p /mnt/disks/my-ssd/pelton
chown -R mysql.mysql /mnt/disks/my-ssd
chmod -R 777 /mnt/disks/my-ssd
