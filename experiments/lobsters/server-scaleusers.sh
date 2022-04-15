#!/bin/bash
cd /home/pelton/pelton

scales=("0.173", "0.863", "1.73", "2.59", "3.45",  "4.312")
users=("1000", "5000", "10000", "15000", "20000", "25000")

echo "pelton ...."
sudo service mariadb stop
for index in "${!scales[@]}";
do
  scale=${scales[$index]}
  users=${users[$index]}
  echo "pelton-$user"
  ./run.sh opt --hostname="10.128.0.18:10001" > log-${users}.txt
done

echo "baseline ..."
sudo service mariadb start
