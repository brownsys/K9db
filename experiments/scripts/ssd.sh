#!/bin/bash
# Stop MariaDB, unmount SSD if mounted.
sudo service mariadb stop || /bin/true
sudo umount /mnt/disks/my-ssd || /bin/true

# Format and mount SSD.
sudo mkfs.ext4 -F /dev/nvme0n1
sudo mkdir -p /mnt/disks/my-ssd
sudo mount /dev/nvme0n1 /mnt/disks/my-ssd

# Make SSD owned by mysql user.
sudo chown mysql.mysql /mnt/disks/my-ssd
sudo cp /var/lib/mysql /mnt/disks/my-ssd/ -r
sudo chown -R mysql.mysql /mnt/disks/my-ssd

# Create a k9db directory in SSD and make it accessible to any user.
sudo mkdir -p /mnt/disks/my-ssd/k9db
sudo chmod -R 777 /mnt/disks/my-ssd
